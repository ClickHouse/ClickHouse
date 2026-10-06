"""
Regression test for https://github.com/ClickHouse/ClickHouse/issues/90651

`CREATE TABLE ... ON CLUSTER ... AS SELECT ... FROM <Distributed> ORDER BY ...`
used to fail on a cluster with two or more shards with

    Code: 1001 ... std::bad_function_call            (25.8)
    Code: 10 ... Not found column __table1.<col> in block. There are only columns: <col>:
        While executing Remote. (NOT_FOUND_COLUMN_IN_BLOCK)   (later versions)

Root cause: the `ON CLUSTER` DDL is executed by the DDL worker, whose query
context did not carry a client version (it stayed 0.0.0). When that context
reads the `Distributed` table it forwards the sub-query to the remote shard as
if the initiator were a pre-23.3 server, so the shard disabled the analyzer for
"compatibility". The initiator kept using the analyzer, so the initiator and the
shard produced different column names (`__table1.x` vs `x`) and distributed
execution failed.

The query is only affected when at least one shard is read remotely, and the
outcome depended on whether `allow_experimental_analyzer` happened to be marked
as explicitly changed, so it could look intermittent. The loop below runs the
statement several times to be robust to that. Because the user-visible failure
depends on that `changed` flag, an unfixed server may still execute the
statement successfully; `test_ddl_worker_context_carries_client_version`
distinguishes a fixed server from an unfixed one deterministically by checking
the client version that the DDL worker's query context records in `query_log`.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": 2, "replica": 1},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in (node1, node2):
            node.query(
                "CREATE TABLE local (num UInt64, d Date) "
                "ENGINE = MergeTree ORDER BY num"
            )
            node.query(
                "CREATE TABLE dist (num Int32, d Date) "
                "ENGINE = Distributed(test_cluster, currentDatabase(), local, cityHash64(num))"
            )
        node1.query("INSERT INTO local SELECT number, today() FROM numbers(50000)")
        node2.query(
            "INSERT INTO local SELECT number + 50000, today() FROM numbers(50000)"
        )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "select",
    [
        # Minimal reproducer: a raw column from a Distributed table plus a step
        # (ORDER BY) that forces the coordinator to merge the shard streams.
        "SELECT num FROM dist ORDER BY num",
        # A window function forces the same coordinator merge.
        "SELECT num, count() OVER (PARTITION BY num % 4) AS c FROM dist ORDER BY num",
        # The shape from the original report: cross join + window + ORDER BY.
        "SELECT num, jn.n, uniq(num) OVER (PARTITION BY jn.n) AS m "
        "FROM dist CROSS JOIN (SELECT number AS n FROM numbers(4)) AS jn "
        "WHERE num != 0 ORDER BY jn.n",
        # An aggregation forces the coordinator to merge per-shard states.
        "SELECT num % 10 AS k, count() AS c FROM dist GROUP BY k ORDER BY k",
    ],
)
def test_create_as_select_on_cluster_over_distributed(started_cluster, select):
    # The failure looked intermittent, so run the statement several times.
    for i in range(10):
        table = f"res_{abs(hash(select)) % 100000}_{i}"
        node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER test_cluster SYNC")
        # This must not throw NOT_FOUND_COLUMN_IN_BLOCK / bad_function_call.
        node1.query(
            f"CREATE TABLE {table} ON CLUSTER test_cluster ENGINE = Memory AS {select}"
        )
        # Every shard executed the same statement locally and must hold data.
        assert int(node1.query(f"SELECT count() FROM {table}")) > 0
        assert int(node2.query(f"SELECT count() FROM {table}")) > 0
        node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER test_cluster SYNC")


def test_ddl_worker_context_carries_client_version(started_cluster):
    """The DDL worker's query context must carry a real client version.

    Before the fix it stayed 0.0.0, which is the root cause of the issue above:
    a remote shard treated the initiator of the forwarded sub-query as a
    pre-23.3 server and applied compatibility downgrades (in particular,
    disabling the analyzer). Whether that downgrade actually fires depends on
    whether the environment happens to mark `allow_experimental_analyzer` as
    explicitly changed, so the end-to-end test above may pass even on an
    unfixed server. The client version recorded in `system.query_log` for the
    query executed by the DDL worker (identified by the `/* ddl_entry=... */`
    prefix that the DDL worker prepends) distinguishes the two
    deterministically: it is 0 without the fix and the server's own version
    with it.
    """
    table = "res_ddl_client_version"
    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER test_cluster SYNC")
    node1.query(
        f"CREATE TABLE {table} ON CLUSTER test_cluster ENGINE = Memory "
        "AS SELECT num FROM dist ORDER BY num"
    )
    for node in (node1, node2):
        node.query("SYSTEM FLUSH LOGS")
        # The fix stamps the DDL worker context with the server's own
        # compile-time version (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH),
        # whose first three components are exactly what `version()` reports.
        server_version = tuple(node.query("SELECT version()").strip().split(".")[:3])
        rows = node.query(
            "SELECT DISTINCT client_version_major, client_version_minor, "
            "client_version_patch FROM system.query_log "
            "WHERE type = 'QueryFinish' AND query_kind = 'Create' "
            f"AND query LIKE '/* ddl_entry=query-%' AND query LIKE '%{table}%'"
        ).splitlines()
        assert rows, f"no DDL-executed CREATE found in query_log on {node.name}"
        for row in rows:
            logged = tuple(row.split("\t"))
            # It is 0.0.0 without the fix. Asserting the exact server version
            # - not merely a non-zero major - proves the DDL worker forwards a
            # version far above the 23.3.0 compatibility threshold in
            # TCPHandler, so remote shards do not apply the pre-23.3 analyzer
            # downgrade. A broken implementation stamping e.g. 1.0.0 or 22.8.0
            # would pass a `major > 0` check yet still trip that downgrade.
            assert logged == server_version, (
                f"the DDL worker on {node.name} executed a query whose context "
                f"carries client version {'.'.join(logged)} instead of the "
                f"server's own {'.'.join(server_version)}; a version below "
                "23.3.0 (in particular 0.0.0) makes remote shards apply "
                "pre-23.3 compatibility downgrades"
            )
    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER test_cluster SYNC")
