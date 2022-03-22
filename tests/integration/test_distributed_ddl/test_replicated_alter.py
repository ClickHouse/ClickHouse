import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.test_tools import TSV
from .cluster import ClickHouseClusterWithDDLHelpers


@pytest.fixture(scope="module", params=["configs", "configs_secure"])
def test_cluster(request):
    cluster = ClickHouseClusterWithDDLHelpers(
        __file__, request.param, "alters_" + request.param
    )

    try:
        # TODO: Fix ON CLUSTER alters when nodes have different configs. Need to canonicalize node identity.
        cluster.prepare(replace_hostnames_with_ips=False)

        yield cluster

        instance = cluster.instances["ch1"]
        cluster.ddl_check_query(instance, "DROP DATABASE test ON CLUSTER 'cluster'")
        cluster.ddl_check_query(
            instance, "DROP DATABASE IF EXISTS test2 ON CLUSTER 'cluster'"
        )

        # Check query log to ensure that DDL queries are not executed twice
        time.sleep(1.5)
        for instance in list(cluster.instances.values()):
            cluster.ddl_check_there_are_no_dublicates(instance)

        cluster.pm_random_drops.heal_all()

    finally:
        cluster.shutdown()


def test_replicated_alters(test_cluster):
    instance = test_cluster.instances["ch2"]

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS merge_for_alter ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS all_merge_32 ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS all_merge_64 ON CLUSTER cluster SYNC"
    )

    # Temporarily disable random ZK packet drops, they might broke creation if ReplicatedMergeTree replicas
    firewall_drops_rules = test_cluster.pm_random_drops.pop_rules()

    test_cluster.ddl_check_query(
        instance,
        """
CREATE TABLE IF NOT EXISTS merge_for_alter ON CLUSTER cluster (p Date, i Int32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}', p, p, 1)
""",
    )

    test_cluster.ddl_check_query(
        instance,
        """
CREATE TABLE IF NOT EXISTS all_merge_32 ON CLUSTER cluster (p Date, i Int32)
ENGINE = Distributed(cluster, default, merge_for_alter, i)
""",
    )
    test_cluster.ddl_check_query(
        instance,
        """
CREATE TABLE IF NOT EXISTS all_merge_64 ON CLUSTER cluster (p Date, i Int64, s String)
ENGINE = Distributed(cluster, default, merge_for_alter, i)
""",
    )

    for i in range(4):
        k = (i // 2) * 2
        test_cluster.insert_reliable(
            test_cluster.instances["ch{}".format(i + 1)],
            "INSERT INTO merge_for_alter (i) VALUES ({})({})".format(k, k + 1),
        )

    test_cluster.sync_replicas("merge_for_alter")

    assert TSV(instance.query("SELECT i FROM all_merge_32 ORDER BY i")) == TSV(
        "".join(["{}\n".format(x) for x in range(4)])
    )

    test_cluster.ddl_check_query(
        instance, "ALTER TABLE merge_for_alter ON CLUSTER cluster MODIFY COLUMN i Int64"
    )
    test_cluster.ddl_check_query(
        instance,
        "ALTER TABLE merge_for_alter ON CLUSTER cluster ADD COLUMN s String DEFAULT toString(i)",
    )

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(
        "".join(["{}\t{}\n".format(x, x) for x in range(4)])
    )

    for i in range(4):
        k = (i // 2) * 2 + 4
        test_cluster.insert_reliable(
            test_cluster.instances["ch{}".format(i + 1)],
            "INSERT INTO merge_for_alter (p, i) VALUES (31, {})(31, {})".format(
                k, k + 1
            ),
        )

    test_cluster.sync_replicas("merge_for_alter")

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(
        "".join(["{}\t{}\n".format(x, x) for x in range(8)])
    )

    test_cluster.ddl_check_query(
        instance,
        "ALTER TABLE merge_for_alter ON CLUSTER cluster DETACH PARTITION 197002",
    )
    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(
        "".join(["{}\t{}\n".format(x, x) for x in range(4)])
    )

    test_cluster.ddl_check_query(
        instance, "DROP TABLE merge_for_alter ON CLUSTER cluster SYNC"
    )

    # Enable random ZK packet drops
    test_cluster.pm_random_drops.push_rules(firewall_drops_rules)

    test_cluster.ddl_check_query(
        instance, "DROP TABLE all_merge_32 ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE all_merge_64 ON CLUSTER cluster SYNC"
    )
