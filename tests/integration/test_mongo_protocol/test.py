# -*- coding: utf-8 -*-

import datetime
import decimal
import logging
import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
import pymongo

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
cluster.add_instance(
    "node",
    main_configs=[
        "configs/mongo.xml",
        "configs/log.xml",
        "configs/users.xml",
    ],
    user_configs=["configs/default_password.xml"],
    with_mongo=True,
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
)

server_port = 27017


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Wait for the Mongo handler to start.
        # Cluster.start waits until port 9000 becomes accessible.
        # Server opens the Mongo compatibility port a bit later.
        cluster.instances["node"].wait_for_log_line("Mongo compatibility protocol")
        yield cluster
    except Exception as ex:
        logging.exception(ex)
        raise ex
    finally:
        cluster.shutdown()


def test_count_query(started_cluster):
    node = cluster.instances["node"]
    client = pymongo.MongoClient(f"mongodb://default:123@{node.ip_address}:{server_port}/default?authMechanism=PLAIN")
    db = client['db']
    collection = db['count']

    collection.drop()
    documents = [
        {"name": "Bob Johnson", "age": 32, "city": 1},
        {"name": "Charlie Brown", "age": 24, "city": 2},
        {"name": "David Williams", "age": 40, "city": 3}
    ]
    collection.insert_many(documents)

    assert collection.estimated_document_count() == 3    

    collection.delete_many({"age" : 24})

    assert collection.estimated_document_count() == 2


def test_find_query(started_cluster):
    node = cluster.instances["node"]
    client = pymongo.MongoClient(f"mongodb://default:123@{node.ip_address}:{server_port}/default?authMechanism=PLAIN")
    db = client['db']
    collection = db['find']

    collection.drop()
    documents = [
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "David Williams", "age": 40, "city": "Chicago"}
    ]
    collection.insert_many(documents)

    find_docs = [doc for doc in collection.find({})]
    find_docs = sorted(find_docs, key=lambda x: x["age"])

    assert find_docs == [
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "David Williams", "age": 40, "city": "Chicago"}
    ]

    find_docs = [doc for doc in collection.find({}).limit(2)]
    assert len(find_docs) == 2

    find_docs = [doc for doc in collection.find({"age" : 24})]
    assert find_docs == [
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
    ]

    find_docs = [doc for doc in collection.find(projection = {"abacaba" : "name"})]
    find_docs = sorted(find_docs, key=lambda x: x["abacaba"])
    assert find_docs == [
        {'abacaba': 'Bob Johnson'},
        {'abacaba': 'Charlie Brown'},
        {'abacaba': 'David Williams'},
    ]

    find_docs = [doc for doc in collection.find().sort("city",1)]
    assert find_docs == [
        {"name": "David Williams", "age": 40, "city": "Chicago"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
    ]

    collection.update_many(
        {'age': 32},
        {"$set" : {"city" : 42}}
    )


def test_index(started_cluster):
    node = cluster.instances["node"]
    client = pymongo.MongoClient(f"mongodb://default:123@{node.ip_address}:{server_port}/default?authMechanism=PLAIN")
    db = client['db']
    collection = db['index']

    collection.drop()
    documents = [
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "David Williams", "age": 40, "city": "Chicago"}
    ]
    collection.insert_many(documents)

    collection.create_index("age")
    find_docs = [doc for doc in collection.find({})]
    find_docs = sorted(find_docs, key=lambda x: x["age"])

    assert find_docs == [
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "David Williams", "age": 40, "city": "Chicago"}
    ]
