import pytest

from helpers.cluster import (
    CLICKHOUSE_CI_PRE_NULLABLE_TUPLE_VERSION,
    ClickHouseCluster,
)

cluster = ClickHouseCluster(__file__)
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
    pre_nullable_tuple_node_1.restart_with_original_version(clear_data_dir=True)


# Compatibility check for aggregate functions that return tuples.
# Before 26.1 many of these functions returned `Tuple(...)`.
# Since 26.1 they may return `Nullable(Tuple(...))` when nullable arguments are used.
# This test checks that mixed-version distributed execution stays compatible across that change.
def test_backward_compatibility_for_tuple_return_type(start_cluster):
    mixed_nodes = (pre_nullable_tuple_node_1, pre_nullable_tuple_node_2, node3, node4)
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
            # -If combinator
            (
                "sumCountIf",
                f"SELECT sumCountIf(x, x > 2) FROM {remote_tab}",
            ),
            (
                "simpleLinearRegressionIf",
                f"SELECT tuple("
                f"roundBankers(tupleElement(simpleLinearRegressionIf(x, y, x > 1), 1), 4), "
                f"roundBankers(tupleElement(simpleLinearRegressionIf(x, y, x > 1), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "studentTTestIf",
                f"SELECT tuple("
                f"roundBankers(tupleElement(studentTTestIf(x, g, x > 1), 1), 4), "
                f"roundBankers(tupleElement(studentTTestIf(x, g, x > 1), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMinIf",
                f"SELECT argAndMinIf(a, b, a > 0) FROM {remote_tab}",
            ),
            (
                "argAndMaxIf",
                f"SELECT argAndMaxIf(a, b, a > 0) FROM {remote_tab}",
            ),
            # -Distinct combinator
            (
                "sumCountDistinct",
                f"SELECT sumCountDistinct(x) FROM {remote_tab}",
            ),
            (
                "simpleLinearRegressionDistinct",
                f"SELECT tuple("
                f"roundBankers(tupleElement(simpleLinearRegressionDistinct(x, y), 1), 4), "
                f"roundBankers(tupleElement(simpleLinearRegressionDistinct(x, y), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "studentTTestDistinct",
                f"SELECT tuple("
                f"roundBankers(tupleElement(studentTTestDistinct(x, g), 1), 4), "
                f"roundBankers(tupleElement(studentTTestDistinct(x, g), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMinDistinct",
                f"SELECT argAndMinDistinct(a, b) FROM {remote_tab}",
            ),
            (
                "argAndMaxDistinct",
                f"SELECT argAndMaxDistinct(a, b) FROM {remote_tab}",
            ),
            # -DistinctIf combinator
            (
                "sumCountDistinctIf",
                f"SELECT sumCountDistinctIf(x, x > 2) FROM {remote_tab}",
            ),
            (
                "simpleLinearRegressionDistinctIf",
                f"SELECT tuple("
                f"roundBankers(tupleElement(simpleLinearRegressionDistinctIf(x, y, x > 1), 1), 4), "
                f"roundBankers(tupleElement(simpleLinearRegressionDistinctIf(x, y, x > 1), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMinDistinctIf",
                f"SELECT argAndMinDistinctIf(a, b, a > 0) FROM {remote_tab}",
            ),
            # -Array combinator
            (
                "sumCountArray",
                f"SELECT sumCountArray([x]) FROM {remote_tab}",
            ),
            (
                "simpleLinearRegressionArray",
                f"SELECT tuple("
                f"roundBankers(tupleElement(simpleLinearRegressionArray([x], [y]), 1), 4), "
                f"roundBankers(tupleElement(simpleLinearRegressionArray([x], [y]), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMinArray",
                f"SELECT argAndMinArray([a], [b]) FROM {remote_tab}",
            ),
            # -Merge combinator
            (
                "sumCountMerge",
                f"SELECT sumCountMerge(s) FROM (SELECT sumCountState(x) AS s FROM {remote_tab})",
            ),
            (
                "simpleLinearRegressionMerge",
                f"SELECT tuple("
                f"roundBankers(tupleElement(simpleLinearRegressionMerge(s), 1), 4), "
                f"roundBankers(tupleElement(simpleLinearRegressionMerge(s), 2), 4)) "
                f"FROM (SELECT simpleLinearRegressionState(x, y) AS s FROM {remote_tab})",
            ),
            (
                "argAndMinMerge",
                f"SELECT argAndMinMerge(s) FROM (SELECT argAndMinState(a, b) AS s FROM {remote_tab})",
            ),
            # -ForEach combinator
            (
                "sumCountForEach",
                f"SELECT sumCountForEach([x]) FROM {remote_tab}",
            ),
            (
                "simpleLinearRegressionForEach",
                f"SELECT arrayMap(t -> tuple("
                f"roundBankers(tupleElement(t, 1), 4), "
                f"roundBankers(tupleElement(t, 2), 4)), "
                f"simpleLinearRegressionForEach([x], [y])) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMinForEach",
                f"SELECT argAndMinForEach([a], [b]) FROM {remote_tab}",
            ),
            # -Resample combinator
            (
                "sumCountResample",
                f"SELECT sumCountResample(0, 2, 1)(x, g) FROM {remote_tab}",
            ),
            (
                "simpleLinearRegressionResample",
                f"SELECT arrayMap(t -> tuple("
                f"roundBankers(tupleElement(t, 1), 4), "
                f"roundBankers(tupleElement(t, 2), 4)), "
                f"simpleLinearRegressionResample(0, 2, 1)(x, y, g)) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMinResample",
                f"SELECT argAndMinResample(0, 2, 1)(a, b, g) FROM {remote_tab}",
            ),
            # -OrDefault combinator
            (
                "sumCountOrDefault",
                f"SELECT sumCountOrDefault(x) FROM {remote_tab}",
            ),
            (
                "simpleLinearRegressionOrDefault",
                f"SELECT tuple("
                f"roundBankers(tupleElement(simpleLinearRegressionOrDefault(x, y), 1), 4), "
                f"roundBankers(tupleElement(simpleLinearRegressionOrDefault(x, y), 2), 4)) "
                f"FROM {remote_tab}",
            ),
            (
                "argAndMinOrDefault",
                f"SELECT argAndMinOrDefault(a, b) FROM {remote_tab}",
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
            ("sumCountIf_empty", f"SELECT sumCountIf(x, x > 2) FROM {remote_tab} WHERE 0"),
            ("sumCountDistinct_empty", f"SELECT sumCountDistinct(x) FROM {remote_tab} WHERE 0"),
            ("sumCountArray_empty", f"SELECT sumCountArray([x]) FROM {remote_tab} WHERE 0"),
            (
                "simpleLinearRegressionIf_empty",
                f"SELECT simpleLinearRegressionIf(x, y, x > 1) FROM {remote_tab} WHERE 0",
            ),
            ("argAndMinIf_empty", f"SELECT argAndMinIf(a, b, a > 0) FROM {remote_tab} WHERE 0"),
            ("sumCountForEach_empty", f"SELECT sumCountForEach([x]) FROM {remote_tab} WHERE 0"),
            ("sumCountResample_empty", f"SELECT sumCountResample(0, 2, 1)(x, g) FROM {remote_tab} WHERE 0"),
            ("sumCountOrDefault_empty", f"SELECT sumCountOrDefault(x) FROM {remote_tab} WHERE 0"),
        ]

    def batch_query(checks):
        """Combine individual checks into a single multi-statement query.

        Each check's SELECT is wrapped as ``SELECT 'name', (original_select)``
        so that the output contains one labeled row per check.
        """
        parts = []
        for name, query in checks:
            # Strip trailing semicolons/whitespace from the original query.
            q = query.rstrip().rstrip(";")
            parts.append(f"SELECT '{name}', ({q})")
        return ";\n".join(parts)

    def parse_batch_result(result, checks):
        """Parse the labeled output back into a ``{name: value}`` dict."""
        lines = result.strip().split("\n")
        assert len(lines) == len(checks), (
            f"Expected {len(checks)} result lines, got {len(lines)}"
        )
        parsed = {}
        for line, (name, _) in zip(lines, checks):
            parts = line.split("\t", 1)
            assert parts[0] == name, f"Expected label '{name}', got '{parts[0]}'"
            parsed[name] = parts[1] if len(parts) > 1 else ""
        return parsed

    def run_checks(node, checks):
        """Run all checks on a node in a single round-trip and return results dict."""
        return parse_batch_result(node.query(batch_query(checks)), checks)

    mixed_checks = build_checks(mixed_remote_tab)
    pre_nullable_only_checks = build_checks(pre_nullable_only_remote_tab)
    mixed_empty_checks = build_empty_checks(mixed_remote_tab)
    pre_nullable_only_empty_checks = build_empty_checks(pre_nullable_only_remote_tab)

    # Baseline: run all checks from the first pre-26.1 node.
    mixed_baseline = run_checks(pre_nullable_tuple_node_1, mixed_checks)
    assert mixed_baseline["sumCount"] == "(48,16)"

    # Verify all other nodes produce the same results (one round-trip per node).
    for node in [pre_nullable_tuple_node_2, node3, node4]:
        results = run_checks(node, mixed_checks)
        for name in mixed_baseline:
            assert results[name] == mixed_baseline[name], f"{name} on {node.name}"

    # Extra baseline with 4x pre-26.1 nodes — must match mixed baseline.
    pre_nullable_only_baseline = run_checks(
        pre_nullable_tuple_node_1, pre_nullable_only_checks
    )
    for name in mixed_baseline:
        assert pre_nullable_only_baseline[name] == mixed_baseline[name], name

    # Cover empty aggregate states.
    mixed_empty_baseline = run_checks(pre_nullable_tuple_node_1, mixed_empty_checks)
    assert mixed_empty_baseline["sumCount_empty"] == "(0,0)"

    for node in [pre_nullable_tuple_node_2, node3, node4]:
        results = run_checks(node, mixed_empty_checks)
        for name in mixed_empty_baseline:
            assert results[name] == mixed_empty_baseline[name], f"{name} on {node.name}"

    pre_nullable_only_empty_baseline = run_checks(
        pre_nullable_tuple_node_1, pre_nullable_only_empty_checks
    )
    for name in mixed_empty_baseline:
        assert (
            pre_nullable_only_empty_baseline[name] == mixed_empty_baseline[name]
        ), name

    # Upgrade one pre-26.1 node to latest and re-check compatibility from all coordinators.
    pre_nullable_tuple_node_1.restart_with_latest_version(fix_metadata=True)

    for node in [pre_nullable_tuple_node_1, pre_nullable_tuple_node_2, node3, node4]:
        results = run_checks(node, mixed_checks)
        for name in mixed_baseline:
            assert results[name] == mixed_baseline[name], f"{name} on {node.name}"

        results = run_checks(node, mixed_empty_checks)
        for name in mixed_empty_baseline:
            assert (
                results[name] == mixed_empty_baseline[name]
            ), f"{name} on {node.name}"

    for node in test_nodes:
        node.query("DROP TABLE IF EXISTS tab_tuple_return")
