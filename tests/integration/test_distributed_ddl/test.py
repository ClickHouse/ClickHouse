import os
import sys
import time
from contextlib import contextmanager

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.test_tools import TSV
from .cluster import ClickHouseClusterWithDDLHelpers


@pytest.fixture(scope="module", params=["configs", "configs_secure"])
def test_cluster(request):
    cluster = ClickHouseClusterWithDDLHelpers(__file__, request.param, request.param)

    try:
        cluster.prepare()

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


def test_default_database(test_cluster):
    instance = test_cluster.instances["ch3"]

    test_cluster.ddl_check_query(
        instance, "CREATE DATABASE IF NOT EXISTS test2 ON CLUSTER 'cluster' FORMAT TSV"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS null ON CLUSTER 'cluster' FORMAT TSV"
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE null ON CLUSTER 'cluster2' (s String DEFAULT 'escape\t\nme') ENGINE = Null",
        settings={"distributed_ddl_entry_format_version": 2}
    )

    contents = instance.query(
        "SELECT hostName() AS h, database FROM all_tables WHERE name = 'null' ORDER BY h"
    )
    assert TSV(contents) == TSV("ch1\tdefault\nch2\ttest2\nch3\tdefault\nch4\ttest2\n")

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS null ON CLUSTER cluster2",
        settings={"distributed_ddl_entry_format_version": 2}
    )
    test_cluster.ddl_check_query(
        instance, "DROP DATABASE IF EXISTS test2 ON CLUSTER 'cluster'"
    )


def test_create_view(test_cluster):
    instance = test_cluster.instances["ch3"]
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test.super_simple_view ON CLUSTER 'cluster'"
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE VIEW test.super_simple_view ON CLUSTER 'cluster' AS SELECT * FROM system.numbers FORMAT TSV",
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE MATERIALIZED VIEW test.simple_mat_view ON CLUSTER 'cluster' ENGINE = Memory AS SELECT * FROM system.numbers FORMAT TSV",
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE test.simple_mat_view ON CLUSTER 'cluster' FORMAT TSV"
    )
    test_cluster.ddl_check_query(
        instance,
        "DROP TABLE IF EXISTS test.super_simple_view2 ON CLUSTER 'cluster' FORMAT TSV",
    )

    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test.super_simple ON CLUSTER 'cluster' (i Int8) ENGINE = Memory",
    )
    test_cluster.ddl_check_query(
        instance,
        "RENAME TABLE test.super_simple TO test.super_simple2 ON CLUSTER 'cluster' FORMAT TSV",
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE test.super_simple2 ON CLUSTER 'cluster'"
    )


def test_on_server_fail(test_cluster):
    instance = test_cluster.instances["ch1"]
    kill_instance = test_cluster.instances["ch2"]

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test.test_server_fail ON CLUSTER 'cluster'"
    )

    kill_instance.get_docker_handle().stop()
    request = instance.get_query_request(
        "CREATE TABLE test.test_server_fail ON CLUSTER 'cluster' (i Int8) ENGINE=Null",
        timeout=180,
    )
    kill_instance.get_docker_handle().start()

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test.__nope__ ON CLUSTER 'cluster'"
    )

    # Check query itself
    test_cluster.check_all_hosts_successfully_executed(request.get_answer())

    # And check query artefacts
    contents = instance.query(
        "SELECT hostName() AS h FROM all_tables WHERE database='test' AND name='test_server_fail' ORDER BY h"
    )
    assert TSV(contents) == TSV("ch1\nch2\nch3\nch4\n")

    test_cluster.ddl_check_query(
        instance, "DROP TABLE test.test_server_fail ON CLUSTER 'cluster'"
    )


def test_simple_alters(test_cluster):
    instance = test_cluster.instances["ch2"]

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS merge ON CLUSTER '{cluster}'"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS all_merge_32 ON CLUSTER '{cluster}'"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS all_merge_64 ON CLUSTER '{cluster}'"
    )

    test_cluster.ddl_check_query(
        instance,
        """
CREATE TABLE IF NOT EXISTS merge ON CLUSTER '{cluster}' (p Date, i Int32)
ENGINE = MergeTree(p, p, 1)
""",
    )
    test_cluster.ddl_check_query(
        instance,
        """
CREATE TABLE IF NOT EXISTS all_merge_32 ON CLUSTER '{cluster}' (p Date, i Int32)
ENGINE = Distributed('{cluster}', default, merge, i)
""",
    )
    test_cluster.ddl_check_query(
        instance,
        """
CREATE TABLE IF NOT EXISTS all_merge_64 ON CLUSTER '{cluster}' (p Date, i Int64, s String)
ENGINE = Distributed('{cluster}', default, merge, i)
""",
    )

    for i in range(0, 4, 2):
        k = (i / 2) * 2
        test_cluster.instances["ch{}".format(i + 1)].query(
            "INSERT INTO merge (i) VALUES ({})({})".format(k, k + 1)
        )

    assert TSV(instance.query("SELECT i FROM all_merge_32 ORDER BY i")) == TSV(
        "".join(["{}\n".format(x) for x in range(4)])
    )

    time.sleep(5)
    test_cluster.ddl_check_query(
        instance, "ALTER TABLE merge ON CLUSTER '{cluster}' MODIFY COLUMN i Int64"
    )
    time.sleep(5)
    test_cluster.ddl_check_query(
        instance,
        "ALTER TABLE merge ON CLUSTER '{cluster}' ADD COLUMN s String DEFAULT toString(i) FORMAT TSV",
    )

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(
        "".join(["{}\t{}\n".format(x, x) for x in range(4)])
    )

    for i in range(0, 4, 2):
        k = (i / 2) * 2 + 4
        test_cluster.instances["ch{}".format(i + 1)].query(
            "INSERT INTO merge (p, i) VALUES (31, {})(31, {})".format(k, k + 1)
        )

    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(
        "".join(["{}\t{}\n".format(x, x) for x in range(8)])
    )

    test_cluster.ddl_check_query(
        instance, "ALTER TABLE merge ON CLUSTER '{cluster}' DETACH PARTITION 197002"
    )
    assert TSV(instance.query("SELECT i, s FROM all_merge_64 ORDER BY i")) == TSV(
        "".join(["{}\t{}\n".format(x, x) for x in range(4)])
    )

    test_cluster.ddl_check_query(instance, "DROP TABLE merge ON CLUSTER '{cluster}'")
    test_cluster.ddl_check_query(
        instance, "DROP TABLE all_merge_32 ON CLUSTER '{cluster}'"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE all_merge_64 ON CLUSTER '{cluster}'"
    )


def test_macro(test_cluster):
    instance = test_cluster.instances["ch2"]
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE tab ON CLUSTER '{cluster}' (value UInt8) ENGINE = Memory",
    )

    for i in range(4):
        test_cluster.insert_reliable(
            test_cluster.instances["ch{}".format(i + 1)],
            "INSERT INTO tab VALUES ({})".format(i),
        )

    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE distr ON CLUSTER '{cluster}' (value UInt8) ENGINE = Distributed('{cluster}', 'default', 'tab', value % 4)",
    )

    assert TSV(instance.query("SELECT value FROM distr ORDER BY value")) == TSV(
        "0\n1\n2\n3\n"
    )
    assert TSV(
        test_cluster.instances["ch3"].query("SELECT value FROM distr ORDER BY value")
    ) == TSV("0\n1\n2\n3\n")

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS distr ON CLUSTER '{cluster}'"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS tab ON CLUSTER '{cluster}'"
    )


def test_implicit_macros(test_cluster):
    # Temporarily disable random ZK packet drops, they might broke creation if ReplicatedMergeTree replicas
    firewall_drops_rules = test_cluster.pm_random_drops.pop_rules()

    instance = test_cluster.instances["ch2"]

    test_cluster.ddl_check_query(
        instance, "DROP DATABASE IF EXISTS test_db ON CLUSTER '{cluster}' SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "CREATE DATABASE IF NOT EXISTS test_db ON CLUSTER '{cluster}'"
    )

    test_cluster.ddl_check_query(
        instance,
        """
CREATE TABLE IF NOT EXISTS test_db.test_macro ON CLUSTER '{cluster}' (p Date, i Int32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{layer}-{shard}/{table}', '{replica}', p, p, 1)
""",
    )

    # Check that table was created at correct path in zookeeper
    assert (
        test_cluster.get_kazoo_client("zoo1").exists(
            "/clickhouse/tables/test_db/0-1/test_macro"
        )
        is not None
    )

    # Enable random ZK packet drops
    test_cluster.pm_random_drops.push_rules(firewall_drops_rules)


def test_allowed_databases(test_cluster):
    instance = test_cluster.instances["ch2"]
    instance.query("CREATE DATABASE IF NOT EXISTS db1 ON CLUSTER cluster")
    instance.query("CREATE DATABASE IF NOT EXISTS db2 ON CLUSTER cluster")

    instance.query(
        "CREATE TABLE db1.t1 ON CLUSTER cluster (i Int8) ENGINE = Memory",
        settings={"user": "restricted_user"},
    )

    with pytest.raises(Exception):
        instance.query(
            "CREATE TABLE db2.t2 ON CLUSTER cluster (i Int8) ENGINE = Memory",
            settings={"user": "restricted_user"},
        )
    with pytest.raises(Exception):
        instance.query(
            "CREATE TABLE t3 ON CLUSTER cluster (i Int8) ENGINE = Memory",
            settings={"user": "restricted_user"},
        )
    with pytest.raises(Exception):
        instance.query(
            "DROP DATABASE db2 ON CLUSTER cluster", settings={"user": "restricted_user"}
        )

    instance.query(
        "DROP DATABASE db1 ON CLUSTER cluster", settings={"user": "restricted_user"}
    )


def test_kill_query(test_cluster):
    instance = test_cluster.instances["ch3"]

    test_cluster.ddl_check_query(
        instance, "KILL QUERY ON CLUSTER 'cluster' WHERE NOT elapsed FORMAT TSV"
    )


def test_detach_query(test_cluster):
    instance = test_cluster.instances["ch3"]

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test_attach ON CLUSTER cluster FORMAT TSV"
    )
    test_cluster.ddl_check_query(
        instance, "CREATE TABLE test_attach ON CLUSTER cluster (i Int8)ENGINE = Log"
    )
    test_cluster.ddl_check_query(
        instance, "DETACH TABLE test_attach ON CLUSTER cluster FORMAT TSV"
    )
    test_cluster.ddl_check_query(
        instance, "ATTACH TABLE test_attach ON CLUSTER cluster"
    )


def test_optimize_query(test_cluster):
    instance = test_cluster.instances["ch3"]

    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test_optimize ON CLUSTER cluster FORMAT TSV"
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_optimize ON CLUSTER cluster (p Date, i Int32) ENGINE = MergeTree(p, p, 8192)",
    )
    test_cluster.ddl_check_query(
        instance, "OPTIMIZE TABLE test_optimize ON CLUSTER cluster FORMAT TSV"
    )


def test_create_as_select(test_cluster):
    instance = test_cluster.instances["ch2"]
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_as_select ON CLUSTER cluster ENGINE = Memory AS (SELECT 1 AS x UNION ALL SELECT 2 AS x)",
    )
    assert TSV(instance.query("SELECT x FROM test_as_select ORDER BY x")) == TSV(
        "1\n2\n"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test_as_select ON CLUSTER cluster"
    )


def test_create_reserved(test_cluster):
    instance = test_cluster.instances["ch2"]
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_reserved ON CLUSTER cluster (`p` Date, `image` Nullable(String), `index` Nullable(Float64), `invalidate` Nullable(Int64)) ENGINE = MergeTree(`p`, `p`, 8192)",
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_as_reserved ON CLUSTER cluster ENGINE = Memory AS (SELECT * from test_reserved)",
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test_reserved ON CLUSTER cluster"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test_as_reserved ON CLUSTER cluster"
    )


def test_rename(test_cluster):
    instance = test_cluster.instances["ch1"]
    rules = test_cluster.pm_random_drops.pop_rules()
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS rename_shard ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS rename_new ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS rename_old ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS rename ON CLUSTER cluster SYNC"
    )

    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE rename_shard ON CLUSTER cluster (id Int64, sid String DEFAULT concat('old', toString(id))) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/test_shard', '{replica}') ORDER BY (id)",
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE rename_new ON CLUSTER cluster AS rename_shard ENGINE = Distributed(cluster, default, rename_shard, id % 2)",
    )
    test_cluster.ddl_check_query(
        instance, "RENAME TABLE rename_new TO rename ON CLUSTER cluster;"
    )

    for i in range(10):
        instance.query("insert into rename (id) values ({})".format(i))

    # FIXME ddl_check_query doesnt work for replicated DDDL if replace_hostnames_with_ips=True
    # because replicas use wrong host name of leader (and wrong path in zk) to check if it has executed query
    # so ddl query will always fail on some replicas even if query was actually executed by leader
    # Also such inconsistency in cluster configuration may lead to query duplication if leader suddenly changed
    # because path of lock in zk contains shard name, which is list of host names of replicas
    instance.query(
        "ALTER TABLE rename_shard ON CLUSTER cluster MODIFY COLUMN sid String DEFAULT concat('new', toString(id))",
        ignore_error=True,
    )
    time.sleep(1)

    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE rename_new ON CLUSTER cluster AS rename_shard ENGINE = Distributed(cluster, default, rename_shard, id % 2)",
    )

    instance.query("system stop distributed sends rename")

    for i in range(10, 20):
        instance.query("insert into rename (id) values ({})".format(i))

    test_cluster.ddl_check_query(
        instance,
        "RENAME TABLE rename TO rename_old, rename_new TO rename ON CLUSTER cluster",
    )

    for i in range(20, 30):
        instance.query("insert into rename (id) values ({})".format(i))

    instance.query("system flush distributed rename")
    for name in ["ch1", "ch2", "ch3", "ch4"]:
        test_cluster.instances[name].query("system sync replica rename_shard")

    # system stop distributed sends does not affect inserts into local shard,
    # so some ids in range (10, 20) will be inserted into rename_shard
    assert instance.query("select count(id), sum(id) from rename").rstrip() == "25\t360"
    # assert instance.query("select count(id), sum(id) from rename").rstrip() == "20\t290"
    assert (
        instance.query(
            "select count(id), sum(id) from rename where sid like 'old%'"
        ).rstrip()
        == "15\t115"
    )
    # assert instance.query("select count(id), sum(id) from rename where sid like 'old%'").rstrip() == "10\t45"
    assert (
        instance.query(
            "select count(id), sum(id) from rename where sid like 'new%'"
        ).rstrip()
        == "10\t245"
    )
    test_cluster.pm_random_drops.push_rules(rules)


def test_socket_timeout(test_cluster):
    instance = test_cluster.instances["ch1"]
    # queries should not fail with "Timeout exceeded while reading from socket" in case of EINTR caused by query profiler
    for i in range(0, 100):
        instance.query(
            "select hostName() as host, count() from cluster('cluster', 'system', 'settings') group by host"
        )


def test_replicated_without_arguments(test_cluster):
    rules = test_cluster.pm_random_drops.pop_rules()
    instance = test_cluster.instances["ch1"]
    test_cluster.ddl_check_query(
        instance, "DROP TABLE IF EXISTS test_atomic.rmt ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance, "DROP DATABASE IF EXISTS test_atomic ON CLUSTER cluster SYNC"
    )

    test_cluster.ddl_check_query(
        instance, "CREATE DATABASE test_atomic ON CLUSTER cluster ENGINE=Atomic"
    )
    assert (
        "are supported only for ON CLUSTER queries with Atomic database engine"
        in instance.query_and_get_error(
            "CREATE TABLE test_atomic.rmt (n UInt64, s String) ENGINE=ReplicatedMergeTree ORDER BY n"
        )
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_atomic.rmt ON CLUSTER cluster (n UInt64, s String) ENGINE=ReplicatedMergeTree() ORDER BY n",
    )
    test_cluster.ddl_check_query(
        instance, "DROP TABLE test_atomic.rmt ON CLUSTER cluster SYNC"
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_atomic.rmt UUID '12345678-0000-4000-8000-000000000001' ON CLUSTER cluster (n UInt64, s String) ENGINE=ReplicatedMergeTree ORDER BY n",
    )
    assert (
        instance.query("SHOW CREATE test_atomic.rmt FORMAT TSVRaw")
        == "CREATE TABLE test_atomic.rmt\n(\n    `n` UInt64,\n    `s` String\n)\nENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')\nORDER BY n\nSETTINGS index_granularity = 8192\n"
    )
    test_cluster.ddl_check_query(
        instance,
        "RENAME TABLE test_atomic.rmt TO test_atomic.rmt_renamed ON CLUSTER cluster",
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_atomic.rmt ON CLUSTER cluster (n UInt64, s String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY n",
    )
    test_cluster.ddl_check_query(
        instance,
        "EXCHANGE TABLES test_atomic.rmt AND test_atomic.rmt_renamed ON CLUSTER cluster",
    )
    assert (
        instance.query(
            "SELECT countDistinct(uuid) from clusterAllReplicas('cluster', 'system', 'databases') WHERE uuid != '00000000-0000-0000-0000-000000000000' AND name='test_atomic'"
        )
        == "1\n"
    )
    assert (
        instance.query(
            "SELECT countDistinct(uuid) from clusterAllReplicas('cluster', 'system', 'tables') WHERE uuid != '00000000-0000-0000-0000-000000000000' AND name='rmt'"
        )
        == "1\n"
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_atomic.rrmt ON CLUSTER cluster (n UInt64, m UInt64) ENGINE=ReplicatedReplacingMergeTree(m) ORDER BY n",
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_atomic.rsmt ON CLUSTER cluster (n UInt64, m UInt64, k UInt64) ENGINE=ReplicatedSummingMergeTree((m, k)) ORDER BY n",
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_atomic.rvcmt ON CLUSTER cluster (n UInt64, m Int8, k UInt64) ENGINE=ReplicatedVersionedCollapsingMergeTree(m, k) ORDER BY n",
    )
    test_cluster.ddl_check_query(
        instance, "DROP DATABASE test_atomic ON CLUSTER cluster SYNC"
    )

    test_cluster.ddl_check_query(
        instance,
        "CREATE DATABASE test_ordinary ON CLUSTER cluster ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    assert (
        "are supported only for ON CLUSTER queries with Atomic database engine"
        in instance.query_and_get_error(
            "CREATE TABLE test_ordinary.rmt ON CLUSTER cluster (n UInt64, s String) ENGINE=ReplicatedMergeTree ORDER BY n"
        )
    )
    assert (
        "are supported only for ON CLUSTER queries with Atomic database engine"
        in instance.query_and_get_error(
            "CREATE TABLE test_ordinary.rmt ON CLUSTER cluster (n UInt64, s String) ENGINE=ReplicatedMergeTree('/{shard}/{uuid}/', '{replica}') ORDER BY n"
        )
    )
    test_cluster.ddl_check_query(
        instance,
        "CREATE TABLE test_ordinary.rmt ON CLUSTER cluster (n UInt64, s String) ENGINE=ReplicatedMergeTree('/{shard}/{table}/', '{replica}') ORDER BY n",
    )
    assert (
        instance.query("SHOW CREATE test_ordinary.rmt FORMAT TSVRaw")
        == "CREATE TABLE test_ordinary.rmt\n(\n    `n` UInt64,\n    `s` String\n)\nENGINE = ReplicatedMergeTree('/{shard}/rmt/', '{replica}')\nORDER BY n\nSETTINGS index_granularity = 8192\n"
    )
    test_cluster.ddl_check_query(
        instance, "DROP DATABASE test_ordinary ON CLUSTER cluster SYNC"
    )
    test_cluster.pm_random_drops.push_rules(rules)


if __name__ == "__main__":
    with contextmanager(test_cluster)() as ctx_cluster:
        for name, instance in list(ctx_cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")
