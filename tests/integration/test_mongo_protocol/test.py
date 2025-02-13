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

server_port = 5433


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


def test_pymongo_client(started_cluster):
    node = cluster.instances["node"]
    client = pymongo.MongoClient(node.ip_address, server_port)
    db = client.your_database_name
    collection = db.your_collection_name
    cursor = collection.find({})
    for document in cursor:
        print(document) 
