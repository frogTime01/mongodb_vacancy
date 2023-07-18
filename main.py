import pandas as pd
import numpy as np
from pymongo import MongoClient
from pprint import pprint
from pymongo.errors import DuplicateKeyError


def add_new_unique_vacancy(collection, new_vacancy):
    """I assume that the vacancies are the same if they have the same names and salary"""
    try:
        if not collection.find_one({"name_job": new_vacancy["name_job"], "max_salary": new_vacancy["max_salary"],
                                    "min_salary": new_vacancy["min_salary"]}):
            collection.insert_one(new_vacancy)
        else:
            print("a similar item is already in the collection")
    except DuplicateKeyError:
        print("an item with such a key already exists")


def search_vacancy(collection, min_salary):
    for vac in collection.find({'$or': [{'min_salary': {"$gt": min_salary}},
                                        {'min_salary': {"$lt": min_salary},
                                            'max_salary': {"$gt": min_salary}},
                                        {'min_salary': np.nan, 'max_salary': {
                                            "$gt": min_salary}},
                                        {'max_salary': np.nan}]}):
        pprint(vac)


client = MongoClient('127.0.0.1', 27017)
db_vacancy = client["db_vacancy"]
collection_vacancy = db_vacancy.collection_vacancy
data = pd.read_csv("jobs.csv")
data_dict = data.to_dict("records")
collection_vacancy.insert_many(data_dict)
print(f"initial count = {collection_vacancy.count_documents({})}")

exist_item = {'Unnamed: 0': 13,
              'currency': '₽',
              'link': 'https://hh.ru/vacancy/83184043?from=vacancy_search_list&query=python',
              'max_salary': 250000.0,
              'min_salary': np.nan,
              'name_job': 'Python разработчик'}
new_item = {'Unnamed: 0': 123,
            'currency': '₽',
            'link': 'https://hh.ru/vacancy/31840?from=vacancy_search_list&query=js',
            'max_salary': 235000.0,
            'min_salary': 150000.0,
            'name_job': 'JS разработчик'}

add_new_unique_vacancy(collection_vacancy, exist_item)
add_new_unique_vacancy(collection_vacancy, new_item)
print(f"after adding count = {collection_vacancy.count_documents({})}")

print("vacancies with a salary of more than 150,000: ")
search_vacancy(collection_vacancy, 150000)

db_vacancy.drop_collection('collection_vacancy')
client.close()
