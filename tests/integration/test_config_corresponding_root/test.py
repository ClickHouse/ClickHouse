import os
import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
config_dir = os.path.join(SCRIPT_DIR, './configs')

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', config_dir = config_dir)
caught_exception = ""

@pytest.fixture(scope="module")
def start_cluster():
    global caught_exception
    try:
        cluster.start()
    except Exception as e:
        caught_exception = str(e)

def test_work(start_cluster):
    assert caught_exception.find("Root element doesn't have the corresponding root element as the config file.") != -1
