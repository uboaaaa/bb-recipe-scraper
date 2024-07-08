from lxml import etree
from bs4 import BeautifulSoup
import cloudscraper
import pandas as pd
import time
import random
import json
import re
from pprint import pprint


# Methods
def scrape_recipe(url, scraper):
    print(f"Scraping recipe: {url}")
    try:
        response = scraper.get(url)
        response.raise_for_status()  # TODO turn to function
        soup = BeautifulSoup(response.text, "html.parser")

        # BB recipe pages can be differentiated from other pages by presence of the "recipe card" which contains ingredients, instructions, etc
        recipe_card = soup.find("div", class_="bb-recipe-card")
        if not recipe_card:
            print("Not a recipe page! Skipping...")
            return None

        script = soup.find("script", type="application/ld+json")

        if script:
            json_data = json.loads(script.string)
            if isinstance(json_data, list):
                recipe_data = next(
                    (item for item in json_data if item.get("@type") == "Recipe"), None
                )
            elif isinstance(json_data, dict):
                if "@graph" in json_data:
                    recipe_data = next(
                        (
                            item
                            for item in json_data["@graph"]
                            if item.get("@type") == "Recipe"
                        ),
                        None,
                    )
                elif json_data.get("@type") == "Recipe":
                    recipe_data = json_data
                else:
                    recipe_data = None
            else:
                recipe_data = None

            if not recipe_data:
                print("No Recipe data found in JSON-LD")
                return None

        # Data processing
        cost_span = soup.find("span", class_="cost-per")
        if cost_span:
            cost_raw = [float(f) for f in re.findall(r'[\d\.\d]+', cost_span.text)]
            if len(cost_raw) == 1:
                cost_total = -1
                cost_per = cost_raw[0]
            else:
                cost_total = cost_raw[0]
                cost_per = cost_raw[1]
        else:
            cost_total = -1
            cost_per = -1

        def extract_time(time_string):
            if isinstance(time_string, int):
                return time_string
            return int("".join([d for d in str(time_string) if d.isdigit()] or ["0"]))

        recipe_yield = recipe_data.get("recipeYield", ["", ""])
        if isinstance(recipe_yield, str):
            recipe_yield = [recipe_yield, ""]
        servings = recipe_yield[0]
        serving_unit = (
            recipe_yield[1].replace(servings, '').replace(")", "").replace("(", "").strip()
            if len(recipe_yield) > 1
            else ""
        )
        servings = float(servings)

        prep_time = extract_time(recipe_data.get("prepTime", 0))
        cook_time = extract_time(recipe_data.get("cookTime", 0))
        total_time = extract_time(recipe_data.get("totalTime", prep_time + cook_time))

        ingredients_raw = recipe_data.get("recipeIngredient", [])
        ingredients = [" ".join(i.split()) for i in ingredients_raw] # normalize spacing

        instructions_raw = recipe_data.get("recipeInstructions", [])
        instructions = {}
        step_counter = 1

        def process_instruction(instruction, parent_name=''): #TODO look at this
            nonlocal step_counter
            if isinstance(instruction, dict):
                if instruction.get('@type') == 'HowToSection':
                    section_name = instruction.get('name', '')
                    for item in instruction.get('itemListElement', []):
                        process_instruction(item, section_name)
                elif instruction.get('@type') == 'HowToStep':
                    prefix = f"{parent_name}: " if parent_name else ""
                    instructions[step_counter] = f"{prefix}{instruction.get('text', '')}"
                    step_counter += 1

        for instruction in instructions_raw:
            process_instruction(instruction)

        nutrition = recipe_data.get("nutrition", {})
        del nutrition["@type"]

        notes_raw = soup.find("div", class_="wprm-recipe-notes")
        if notes_raw:
            notes = [elem.text.strip() for elem in notes_raw if elem.text.strip()]
        else:
            notes = []

        keywords = set(recipe_data.get("keywords", "").lower().split(", "))

        return {
            "url": url,
            "name": recipe_data.get("name", ""),
            "rating-avg": float(
                recipe_data.get("aggregateRating", {}).get("ratingValue", -1)
            ),
            "rating-votes": int(
                recipe_data.get("aggregateRating", {}).get("ratingCount", -1)
            ),
            "cost_total": cost_total,
            "cost_per_serving": cost_per,
            "servings": servings,
            "serving-unit": serving_unit,
            "prep-time": prep_time,
            "cook-time": cook_time,
            "total-time": total_time,
            "ingredients": ingredients,
            "instructions": instructions,
            "nutrition-data": nutrition,
            "notes": notes,
            "keywords": keywords
        }

    except Exception as e:
        print(f"Error scraping recipe {url}: {e}")
        exit()


scraper = cloudscraper.create_scraper(browser="chrome")

url = "https://www.budgetbytes.com/classic-tomato-sandwiches/"

pprint(scrape_recipe(url, scraper), sort_dicts=False)
