import os
import sys
import time
from contextlib import contextmanager

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# just import cluster setup from test_didstributed_ddl to avoid code duplication
from test_distributed_ddl.cluster import ClickHouseClusterWithDDLHelpers
from helpers.test_tools import assert_eq_with_retry


@pytest.fixture(scope="module", params=["configs"])
def test_cluster(request):
    cluster = ClickHouseClusterWithDDLHelpers(__file__, request.param)

    try:
        cluster.prepare()

        yield cluster

        instance = cluster.instances['ch1']
        cluster.ddl_check_query(instance, "DROP DATABASE test ON CLUSTER 'cluster'")

        # Check query log to ensure that DDL queries are not executed twice
        time.sleep(1.5)
        for instance in list(cluster.instances.values()):
            cluster.ddl_check_there_are_no_dublicates(instance)

        cluster.pm_random_drops.heal_all()

    finally:
        cluster.shutdown()


def test_ddl_worker_queue_table_entries(test_cluster):
    instance = test_cluster.instances['ch1']
    test_cluster.ddl_check_query(instance, "DROP TABLE IF EXISTS merge ON CLUSTER '{cluster}'")
    test_cluster.ddl_check_query(instance, """
CREATE TABLE IF NOT EXISTS merge ON CLUSTER '{cluster}' (p Date, i Int32)
ENGINE = MergeTree(p, p, 1)
""")
    for i in range(0, 4, 2):
        k = (i / 2) * 2
        test_cluster.instances['ch{}'.format(i + 1)].query("INSERT INTO merge (i) VALUES ({})({})".format(k, k + 1))
    time.sleep(2)
    test_cluster.ddl_check_query(instance, "ALTER TABLE merge ON CLUSTER '{cluster}' DETACH PARTITION 197002")
    time.sleep(2)
    # query the ddl_worker_queue table to ensure that the columns are populated as expected
    assert_eq_with_retry(instance,
                         "SELECT If((SELECT count(*) FROM system.ddl_worker_queue  WHERE cluster='cluster' AND status='finished') > 0, 'ok', 'fail')",
                         "ok\n");
    assert_eq_with_retry(instance,
                         "SELECT If((SELECT count(*) FROM system.ddl_worker_queue  WHERE cluster='cluster' AND status='active') >= 0, 'ok', 'fail')",
                         "ok\n")
    test_cluster.ddl_check_query(instance, "DROP TABLE merge ON CLUSTER '{cluster}'")


if __name__ == '__main__':
    with contextmanager(test_cluster)() as ctx_cluster:
        for name, instance in list(ctx_cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")
