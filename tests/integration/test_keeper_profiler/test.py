import pytest

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import KeeperClient, KeeperException
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1",
    main_configs=["configs/keeper_config1.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/keeper_config2.xml"],
    stay_alive=True,
    with_minio=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/keeper_config3.xml"],
    stay_alive=True,
    with_minio=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_profiler(started_cluster):
    node = cluster.instances["node1"]
    if node.is_built_with_sanitizer():
        return

    node.query(
        "CREATE TABLE t (key UInt32, value String) Engine = ReplicatedMergeTree('/clickhouse-tables/test1', 'r1') ORDER BY key"
    )

    for _ in range(100):
        node.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")

    node.query("system flush logs")
    assert int(node.query("exists system.trace_log"))

    result = node.query(
        """
set allow_introspection_functions=1;
system flush logs;
select cnt from (
    select count() as cnt, formatReadableSize(sum(size)),
            arrayStringConcat(
                arrayMap(x, y -> concat(x, ': ', y), arrayMap(x -> addressToLine(x), trace), arrayMap(x -> demangle(addressToSymbol(x)), trace)),
            '\n') as trace
from system.trace_log where trace_type = ‘Real’ and (trace ilike '%KeeperTCPHandler%' or trace ilike '%KeeperDispatcher%') group by trace order by cnt desc) limit 1;
    """
    )

    if len(result) == 0:
        assert 0 < int(
            node.query(
                """
    set allow_introspection_functions=1;
    system flush logs;
    select sum(cnt) from (
        select count() as cnt, formatReadableSize(sum(size)),
                arrayStringConcat(
                    arrayMap(x, y -> concat(x, ': ', y), arrayMap(x -> addressToLine(x), trace), arrayMap(x -> demangle(addressToSymbol(x)), trace)),
                '\n') as trace
    from system.trace_log where trace_type = ‘Real’ group by trace);
        """
            )
        )
        result = node.query(
            """
    set allow_introspection_functions=1;
    system flush logs;
    select * from (
        select count() as cnt, formatReadableSize(sum(size)),
                arrayStringConcat(
                    arrayMap(x, y -> concat(x, ': ', y), arrayMap(x -> addressToLine(x), trace), arrayMap(x -> demangle(addressToSymbol(x)), trace)),
                '\n') as trace
    from system.trace_log where trace_type = ‘Real’ group by trace);
        """
        )
        print(result)
        assert False

    assert 1 < int(result)
