import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
node3 = cluster.add_instance("node3", with_zookeeper=False, use_old_analyzer=True)
node4 = cluster.add_instance("node4", with_zookeeper=False, use_old_analyzer=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    node1.restart_with_original_version(clear_data_dir=True)


# We will test that serialization of internal state of "avg" function is compatible between different versions.
# TODO Implement versioning of serialization format for aggregate function states.
# NOTE This test is too ad-hoc.


def test_backward_compatability_for_avg(start_cluster):
    node1.query("create table tab (x UInt64) engine = Memory")
    node2.query("create table tab (x UInt64) engine = Memory")
    node3.query("create table tab (x UInt64) engine = Memory")
    node4.query("create table tab (x UInt64) engine = Memory")

    node1.query("INSERT INTO tab VALUES (1)")
    node2.query("INSERT INTO tab VALUES (2)")
    node3.query("INSERT INTO tab VALUES (3)")
    node4.query("INSERT INTO tab VALUES (4)")

    assert (
        node1.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )
    assert (
        node2.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )
    assert (
        node3.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )
    assert (
        node4.query("SELECT avg(x) FROM remote('node{1..4}', default, tab)") == "2.5\n"
    )

    # Also check with persisted aggregate function state

    node1.query("create table state (x AggregateFunction(avg, UInt64)) engine = Log")
    node1.query(
        "INSERT INTO state SELECT avgState(arrayJoin(CAST([1, 2, 3, 4] AS Array(UInt64))))"
    )

    assert node1.query("SELECT avgMerge(x) FROM state") == "2.5\n"

    node1.restart_with_latest_version(fix_metadata=True)

    assert node1.query("SELECT avgMerge(x) FROM state") == "2.5\n"

    node1.query("drop table tab")
    node1.query("drop table state")
    node2.query("drop table tab")
    node3.query("drop table tab")
    node4.query("drop table tab")


@pytest.mark.parametrize("uniq_keys", [1000, 500000])
def test_backward_compatability_for_uniq_exact(start_cluster, uniq_keys):
    node1.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64) Engine = Memory")
    node2.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64) Engine = Memory")
    node3.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64) Engine = Memory")
    node4.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64) Engine = Memory")

    node1.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number FROM numbers_mt(0, {uniq_keys})"
    )
    node2.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number FROM numbers_mt(1, {uniq_keys})"
    )
    node3.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number FROM numbers_mt(2, {uniq_keys})"
    )
    node4.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number FROM numbers_mt(3, {uniq_keys})"
    )

    assert (
        node1.query(
            f"SELECT uniqExact(x) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )
    assert (
        node2.query(
            f"SELECT uniqExact(x) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )
    assert (
        node3.query(
            f"SELECT uniqExact(x) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )
    assert (
        node4.query(
            f"SELECT uniqExact(x) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )

    # Also check with persisted aggregate function state

    node1.query(
        f"CREATE TABLE state_{uniq_keys} (x AggregateFunction(uniqExact, UInt64)) Engine = Log"
    )
    node1.query(
        f"INSERT INTO state_{uniq_keys} SELECT uniqExactState(number) FROM numbers_mt({uniq_keys})"
    )

    assert (
        node1.query(f"SELECT uniqExactMerge(x) FROM state_{uniq_keys}")
        == f"{uniq_keys}\n"
    )

    node1.restart_with_latest_version(fix_metadata=True)

    assert (
        node1.query(f"SELECT uniqExactMerge(x) FROM state_{uniq_keys}")
        == f"{uniq_keys}\n"
    )

    node1.query(f"DROP TABLE state_{uniq_keys}")
    node1.query(f"DROP TABLE tab_{uniq_keys}")
    node2.query(f"DROP TABLE tab_{uniq_keys}")
    node3.query(f"DROP TABLE tab_{uniq_keys}")
    node4.query(f"DROP TABLE tab_{uniq_keys}")


@pytest.mark.parametrize("uniq_keys", [1000, 500000])
def test_backward_compatability_for_uniq_exact_variadic(start_cluster, uniq_keys):
    node1.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64, y UInt64) Engine = Memory")
    node2.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64, y UInt64) Engine = Memory")
    node3.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64, y UInt64) Engine = Memory")
    node4.query(f"CREATE TABLE tab_{uniq_keys} (x UInt64, y UInt64) Engine = Memory")

    node1.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number, number/2 FROM numbers_mt(0, {uniq_keys})"
    )
    node2.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number, number/2 FROM numbers_mt(1, {uniq_keys})"
    )
    node3.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number, number/2 FROM numbers_mt(2, {uniq_keys})"
    )
    node4.query(
        f"INSERT INTO tab_{uniq_keys} SELECT number, number/2 FROM numbers_mt(3, {uniq_keys})"
    )

    assert (
        node1.query(
            f"SELECT uniqExact(x, y) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )
    assert (
        node2.query(
            f"SELECT uniqExact(x, y) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )
    assert (
        node3.query(
            f"SELECT uniqExact(x, y) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )
    assert (
        node4.query(
            f"SELECT uniqExact(x, y) FROM remote('node{{1..4}}', default, tab_{uniq_keys})"
        )
        == f"{uniq_keys + 3}\n"
    )

    # Also check with persisted aggregate function state

    node1.query(
        f"CREATE TABLE state_{uniq_keys} (x AggregateFunction(uniqExact, UInt64, UInt64)) Engine = Log"
    )
    node1.query(
        f"INSERT INTO state_{uniq_keys} SELECT uniqExactState(number, intDiv(number,2)) FROM numbers_mt({uniq_keys})"
    )

    assert (
        node1.query(f"SELECT uniqExactMerge(x) FROM state_{uniq_keys}")
        == f"{uniq_keys}\n"
    )

    node1.restart_with_latest_version(fix_metadata=True)

    assert (
        node1.query(f"SELECT uniqExactMerge(x) FROM state_{uniq_keys}")
        == f"{uniq_keys}\n"
    )

    node1.query(f"DROP TABLE state_{uniq_keys}")
    node1.query(f"DROP TABLE tab_{uniq_keys}")
    node2.query(f"DROP TABLE tab_{uniq_keys}")
    node3.query(f"DROP TABLE tab_{uniq_keys}")
    node4.query(f"DROP TABLE tab_{uniq_keys}")
