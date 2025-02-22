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
    ],
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
    client = pymongo.MongoClient(node.ip_address, server_port)
    db = client['db']
    collection = db['count']

    assert len(db.list_collection_names()) > 0
    assert len(client.list_databases_names()) > 0

    documents = [
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "David Williams", "age": 40, "city": "Chicago"}
    ]
    collection.insert_many(documents)

    assert collection.estimated_document_count({}) == 3

    for doc in collection.find():
        print(doc)

    collection.delete_many({"age" : 24})

    assert collection.estimated_document_count({}) == 2

