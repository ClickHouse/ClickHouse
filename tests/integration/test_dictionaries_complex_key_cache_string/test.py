import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node_memory = cluster.add_instance(
    "node_memory", dictionaries=["configs/dictionaries/complex_key_cache_string.xml"]
)
node_ssd = cluster.add_instance(
    "node_ssd", dictionaries=["configs/dictionaries/ssd_complex_key_cache_string.xml"]
)


@pytest.fixture()
def started_cluster():
    try:
        cluster.start()
        node_memory.query(
            "create table radars_table (radar_id String, radar_ip String, client_id String) engine=MergeTree() order by radar_id"
        )
        node_ssd.query(
            "create table radars_table (radar_id String, radar_ip String, client_id String) engine=MergeTree() order by radar_id"
        )

        yield cluster
    finally:
        cluster.shutdown()
