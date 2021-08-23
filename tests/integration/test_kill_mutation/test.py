# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=line-too-long

import pytest
import time
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', with_zookeeper=True)

@pytest.fixture(scope='module')
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_kill_mutation(started_cluster):
    node.query("""
    create table data (
        key Int,
        index key_idx key type minmax granularity 1
    )
    engine = ReplicatedMergeTree('/ch/{database}/', 'r1')
    order by key
    settings
        min_bytes_for_wide_part=0;

    insert into data values (1);
    """)

    # Remove data skipping index to trigger consistency error
    node.exec_in_container(['bash', '-c', 'rm /var/lib/clickhouse/data/default/data/*/skp_*'])

    with pytest.raises(QueryRuntimeException, match=".*skp_idx_key_idx.idx2 doesn't exist.*"):
        node.query("""
        alter table data materialize index key_idx;
        """, settings={
            'mutations_sync': '2',
        })

    node.query("kill mutation where database = 'default' and table = 'data'")

    # If mutation will not be killed then FILE_DOESNT_EXIST will be triggered endlessly.
    value_before = int(node.query("select value from system.errors where name = 'FILE_DOESNT_EXIST'"))
    assert value_before > 0

    time.sleep(5)
    value_after = int(node.query("select value from system.errors where name = 'FILE_DOESNT_EXIST'"))

    assert value_before == value_after
