import pytest

from helpers.cluster import (
    CLICKHOUSE_CI_MIN_TESTED_VERSION,
    CLICKHOUSE_CI_PRE_NULLABLE_TUPLE_VERSION,
    ClickHouseCluster,
)

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

pre_nullable_tuple_node_1 = cluster.add_instance(
    "pre_nullable_tuple_node_1",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_PRE_NULLABLE_TUPLE_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

pre_nullable_tuple_node_2 = cluster.add_instance(
    "pre_nullable_tuple_node_2",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_PRE_NULLABLE_TUPLE_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

pre_nullable_tuple_node_3 = cluster.add_instance(
    "pre_nullable_tuple_node_3",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_PRE_NULLABLE_TUPLE_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

pre_nullable_tuple_node_4 = cluster.add_instance(
    "pre_nullable_tuple_node_4",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_PRE_NULLABLE_TUPLE_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)



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
    for node in (node1, pre_nullable_tuple_node_1):
        node.restart_with_original_version(clear_data_dir=True)


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


# Compatibility check for aggregate functions that return tuples.
# Before 26.1 many of these functions returned `Tuple(...)`.
# Since 26.1 they may return `Nullable(Tuple(...))` when nullable arguments are used.
# This test checks that mixed-version distributed execution stays compatible across that change.
def test_backward_compatibility_for_tuple_return_type(start_cluster):
    mixed_nodes = (pre_nullable_tuple_node_1, pre_nullable_tuple_node_2, node3, node4)
    pre_nullable_only_nodes = (
        pre_nullable_tuple_node_1,
        pre_nullable_tuple_node_2,
        pre_nullable_tuple_node_3,
        pre_nullable_tuple_node_4,
    )
    test_nodes = mixed_nodes + (pre_nullable_tuple_node_3, pre_nullable_tuple_node_4)
    for node in test_nodes:
        node.query(
            "CREATE TABLE tab_tuple_return"
            "(x Nullable(Float64), y Nullable(Float64), g Nullable(UInt8), "
            "a Nullable(Int32), b Nullable(Int32), c Nullable(Int32), m Nullable(Float64), "
            "keys Array(UInt8), vals64 Array(UInt64), vals8 Array(UInt8)) "
            "ENGINE = Log"
        )

    rowset = (
        "(1, 3, 0, 1, 2, 3, 2, [1], [5], [5]), "
        "(2, 5, 0, NULL, 1, 0, 2, [1], [7], [7]), "
        "(NULL, 7, 1, 3, 4, 5, 2, [2], [11], [11]), "
        "(4, 9, 1, 2, 0, 4, 2, [1], [13], [13]), "
        "(5, 11, 1, 5, 3, 2, 2, [2], [17], [17])"
    )
    for node in test_nodes:
        node.query(
            "INSERT INTO tab_tuple_return "
            "(x, y, g, a, b, c, m, keys, vals64, vals8) VALUES " + rowset
        )

    mixed_remote_tab = "remote('pre_nullable_tuple_node_1,pre_nullable_tuple_node_2,node3,node4', default, tab_tuple_return)"
    pre_nullable_only_remote_tab = "remote('pre_nullable_tuple_node_1,pre_nullable_tuple_node_2,pre_nullable_tuple_node_3,pre_nullable_tuple_node_4', default, tab_tuple_return)"

    def build_checks(remote_tab):
        return [
            (
                "simpleLinearRegression",
                f"SELECT tuple("
                f"roundBankers(tupleElement(simpleLinearRegression(x, y), 1), 4), "
                f"roundBankers(tupleElement(simpleLinearRegression(x, y), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "analysisOfVariance",
                f"SELECT tuple("
                f"roundBankers(tupleElement(analysisOfVariance(x, g), 1), 4), "
                f"roundBankers(tupleElement(analysisOfVariance(x, g), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "kolmogorovSmirnovTest",
                f"SELECT tuple("
                f"roundBankers(tupleElement(kolmogorovSmirnovTest('two-sided')(x, g), 1), 4), "
                f"roundBankers(tupleElement(kolmogorovSmirnovTest('two-sided')(x, g), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "mannWhitneyUTest",
                f"SELECT tuple("
                f"roundBankers(tupleElement(mannWhitneyUTest('two-sided')(x, g), 1), 4), "
                f"roundBankers(tupleElement(mannWhitneyUTest('two-sided')(x, g), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "studentTTest",
                f"SELECT tuple("
                f"roundBankers(tupleElement(studentTTest(x, g), 1), 4), "
                f"roundBankers(tupleElement(studentTTest(x, g), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "welchTTest",
                f"SELECT tuple("
                f"roundBankers(tupleElement(welchTTest(x, g), 1), 4), "
                f"roundBankers(tupleElement(welchTTest(x, g), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "meanZTest",
                f"SELECT tuple("
                f"roundBankers(tupleElement(meanZTest(1., 1., 0.95)(x, g), 1), 4), "
                f"roundBankers(tupleElement(meanZTest(1., 1., 0.95)(x, g), 2), 4), "
                f"roundBankers(tupleElement(meanZTest(1., 1., 0.95)(x, g), 3), 4), "
                f"roundBankers(tupleElement(meanZTest(1., 1., 0.95)(x, g), 4), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "studentTTestOneSample",
                f"SELECT tuple("
                f"roundBankers(tupleElement(studentTTestOneSample(x, m), 1), 4), "
                f"roundBankers(tupleElement(studentTTestOneSample(x, m), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMin",
                f"SELECT argAndMin(a, b) FROM {remote_tab}",
            ),
            (
                "argAndMax",
                f"SELECT argAndMax(a, b) FROM {remote_tab}",
            ),
            (
                "argMin",
                f"SELECT argMin(tuple(a, b), c) FROM {remote_tab}",
            ),
            (
                "argMax",
                f"SELECT argMax(tuple(a, b), c) FROM {remote_tab}",
            ),
            (
                "sumMap",
                f"SELECT sumMap(tuple(keys, vals64)) FROM {remote_tab}",
            ),
            (
                "sumMappedArrays",
                f"SELECT sumMappedArrays(tuple(keys, vals64)) FROM {remote_tab}",
            ),
            (
                "sumMapWithOverflow",
                f"SELECT sumMapWithOverflow(tuple(keys, vals8)) FROM {remote_tab}",
            ),
            (
                "sumCount",
                f"SELECT sumCount(x) FROM {remote_tab}",
            ),
        ]

    def build_empty_checks(remote_tab):
        return [
            ("sumCount_empty", f"SELECT sumCount(x) FROM {remote_tab} WHERE 0"),
            ("sumMap_empty", f"SELECT sumMap(tuple(keys, vals64)) FROM {remote_tab} WHERE 0"),
            (
                "sumMappedArrays_empty",
                f"SELECT sumMappedArrays(tuple(keys, vals64)) FROM {remote_tab} WHERE 0",
            ),
            (
                "sumMapWithOverflow_empty",
                f"SELECT sumMapWithOverflow(tuple(keys, vals8)) FROM {remote_tab} WHERE 0",
            ),
        ]

    mixed_checks = build_checks(mixed_remote_tab)
    pre_nullable_only_checks = build_checks(pre_nullable_only_remote_tab)

    mixed_baseline_results = {}
    for name, query in mixed_checks:
        mixed_baseline_results[name] = pre_nullable_tuple_node_1.query(query)

    assert mixed_baseline_results["sumCount"] == "(48,16)\n"

    for name, query in mixed_checks:
        assert pre_nullable_tuple_node_2.query(query) == mixed_baseline_results[name], name
        assert node3.query(query) == mixed_baseline_results[name], name
        assert node4.query(query) == mixed_baseline_results[name], name

    # Extra baseline with 4x pre-26.1 nodes.
    pre_nullable_only_baseline_results = {}
    for name, query in pre_nullable_only_checks:
        pre_nullable_only_baseline_results[name] = pre_nullable_tuple_node_1.query(query)

    for name, _ in mixed_checks:
        assert pre_nullable_only_baseline_results[name] == mixed_baseline_results[name], name

    # Cover empty aggregate states with deterministic tuple-return functions.
    mixed_empty_checks = build_empty_checks(mixed_remote_tab)
    pre_nullable_only_empty_checks = build_empty_checks(pre_nullable_only_remote_tab)

    mixed_empty_baseline_results = {}
    for name, query in mixed_empty_checks:
        mixed_empty_baseline_results[name] = pre_nullable_tuple_node_1.query(query)

    assert mixed_empty_baseline_results["sumCount_empty"] == "(0,0)\n"

    for name, query in mixed_empty_checks:
        assert pre_nullable_tuple_node_2.query(query) == mixed_empty_baseline_results[name], name
        assert node3.query(query) == mixed_empty_baseline_results[name], name
        assert node4.query(query) == mixed_empty_baseline_results[name], name

    pre_nullable_only_empty_baseline_results = {}
    for name, query in pre_nullable_only_empty_checks:
        pre_nullable_only_empty_baseline_results[name] = pre_nullable_tuple_node_1.query(query)

    for name, _ in mixed_empty_checks:
        assert (
            pre_nullable_only_empty_baseline_results[name]
            == mixed_empty_baseline_results[name]
        ), name

    # Upgrade one pre-26.1 node to latest and re-check compatibility from all coordinators.
    pre_nullable_tuple_node_1.restart_with_latest_version(fix_metadata=True)

    for name, query in mixed_checks:
        assert pre_nullable_tuple_node_1.query(query) == mixed_baseline_results[name], name
        assert pre_nullable_tuple_node_2.query(query) == mixed_baseline_results[name], name
        assert node3.query(query) == mixed_baseline_results[name], name
        assert node4.query(query) == mixed_baseline_results[name], name

    for name, query in mixed_empty_checks:
        assert (
            pre_nullable_tuple_node_1.query(query) == mixed_empty_baseline_results[name]
        ), name
        assert (
            pre_nullable_tuple_node_2.query(query) == mixed_empty_baseline_results[name]
        ), name
        assert node3.query(query) == mixed_empty_baseline_results[name], name
        assert node4.query(query) == mixed_empty_baseline_results[name], name

    for node in test_nodes:
        node.query("DROP TABLE IF EXISTS tab_tuple_return")
