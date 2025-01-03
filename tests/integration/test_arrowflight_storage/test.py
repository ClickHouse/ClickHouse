# coding: utf-8

import os
import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_arrowflight=True,
    stay_alive=True
)

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def arrowflight_check_result(result, reference):
    assert TSV(result) == TSV(reference)


def test_get_data():
    result = node.query(f"SELECT * FROM arrowflight('arrowflight1:5005', 'ABC');")
    assert TSV(result) == TSV('test_value_1\tdata1\nabcadbc\ttext_text_text\n123456789\tdata3\n')
