import pytest

from helpers.cluster import ClickHouseCluster

BASELINE_VERSION = "26.2"

cluster = ClickHouseCluster(__file__)
old_node_1 = cluster.add_instance(
    "old_node_1",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=BASELINE_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)
old_node_2 = cluster.add_instance(
    "old_node_2",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=BASELINE_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)

new_node_1 = cluster.add_instance("new_node_1", with_zookeeper=False, use_old_analyzer=True)
new_node_2 = cluster.add_instance("new_node_2", with_zookeeper=False, use_old_analyzer=True)


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
    old_node_1.restart_with_original_version(clear_data_dir=True)


def test_contingency_backward_compatibility(start_cluster):
    """Backward compatibility for contingency/CrossTab functions.

    The branch adds window-optimized variants with different internal state
    representations but identical serialization format. This test verifies that
    mixed-version distributed queries produce correct results.
    """
    all_nodes = (old_node_1, old_node_2, new_node_1, new_node_2)

    for node in all_nodes:
        node.query(
            "CREATE TABLE tab_contingency"
            "(x UInt8, y UInt8, cond UInt8) "
            "ENGINE = Log"
        )

    rowset = (
        "(1, 3, 1), (2, 5, 0), (3, 7, 1), (4, 9, 0), (5, 11, 1), "
        "(6, 2, 0), (7, 4, 1), (8, 6, 0), (9, 8, 1), (10, 10, 0)"
    )
    for node in all_nodes:
        node.query(
            "INSERT INTO tab_contingency (x, y, cond) VALUES " + rowset
        )

    mixed_remote = "remote('old_node_1,old_node_2,new_node_1,new_node_2', default, tab_contingency)"
    old_only_remote = "remote('old_node_1,old_node_2,old_node_1,old_node_2', default, tab_contingency)"

    def r(expr):
        """Wrap a float expression in roundBankers for deterministic comparison."""
        return f"roundBankers({expr}, 4)"

    def build_checks(remote_tab):
        return [
            # -- Basic functions --
            ("cramersV", f"SELECT {r('cramersV(x, y)')} FROM {remote_tab}"),
            ("cramersVBiasCorrected", f"SELECT {r('cramersVBiasCorrected(x, y)')} FROM {remote_tab}"),
            ("contingency", f"SELECT {r('contingency(x, y)')} FROM {remote_tab}"),
            ("theilsU", f"SELECT {r('theilsU(x, y)')} FROM {remote_tab}"),
            # -- If combinator --
            ("cramersVIf", f"SELECT {r('cramersVIf(x, y, cond)')} FROM {remote_tab}"),
            ("theilsUIf", f"SELECT {r('theilsUIf(x, y, cond)')} FROM {remote_tab}"),
            # -- Distinct combinator --
            ("cramersVDistinct", f"SELECT {r('cramersVDistinct(x, y)')} FROM {remote_tab}"),
            ("theilsUDistinct", f"SELECT {r('theilsUDistinct(x, y)')} FROM {remote_tab}"),
            # -- Array combinator --
            ("cramersVArray", f"SELECT {r('cramersVArray([x], [y])')} FROM {remote_tab}"),
            ("theilsUArray", f"SELECT {r('theilsUArray([x], [y])')} FROM {remote_tab}"),
            # -- ForEach combinator --
            ("cramersVForEach", f"SELECT arrayMap(v -> {r('v')}, cramersVForEach([x], [y])) FROM {remote_tab}"),
            ("theilsUForEach", f"SELECT arrayMap(v -> {r('v')}, theilsUForEach([x], [y])) FROM {remote_tab}"),
            # -- State/Merge combinator --
            (
                "cramersVMerge",
                f"SELECT {r('cramersVMerge(s)')} FROM (SELECT cramersVState(x, y) AS s FROM {remote_tab})",
            ),
            (
                "theilsUMerge",
                f"SELECT {r('theilsUMerge(s)')} FROM (SELECT theilsUState(x, y) AS s FROM {remote_tab})",
            ),
            # -- OrDefault combinator --
            ("cramersVOrDefault", f"SELECT {r('cramersVOrDefault(x, y)')} FROM {remote_tab}"),
            ("theilsUOrDefault", f"SELECT {r('theilsUOrDefault(x, y)')} FROM {remote_tab}"),
            # -- OrNull combinator --
            ("cramersVOrNull", f"SELECT {r('cramersVOrNull(x, y)')} FROM {remote_tab}"),
            ("theilsUOrNull", f"SELECT {r('theilsUOrNull(x, y)')} FROM {remote_tab}"),
            # -- Resample combinator --
            (
                "cramersVResample",
                f"SELECT arrayMap(v -> {r('v')}, cramersVResample(0, 20, 10)(x, y, x)) FROM {remote_tab}",
            ),
            (
                "theilsUResample",
                f"SELECT arrayMap(v -> {r('v')}, theilsUResample(0, 20, 10)(x, y, x)) FROM {remote_tab}",
            ),
        ]

    def build_empty_checks(remote_tab):
        return [
            ("cramersV_empty", f"SELECT {r('cramersV(x, y)')} FROM {remote_tab} WHERE 0"),
            ("theilsU_empty", f"SELECT {r('theilsU(x, y)')} FROM {remote_tab} WHERE 0"),
            ("cramersVIf_empty", f"SELECT {r('cramersVIf(x, y, cond)')} FROM {remote_tab} WHERE 0"),
            ("cramersVOrDefault_empty", f"SELECT {r('cramersVOrDefault(x, y)')} FROM {remote_tab} WHERE 0"),
        ]

    def batch_query(checks):
        parts = []
        for name, query in checks:
            q = query.rstrip().rstrip(";")
            parts.append(f"SELECT '{name}', ({q})")
        return ";\n".join(parts)

    def parse_batch_result(result, checks):
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
        return parse_batch_result(node.query(batch_query(checks)), checks)

    mixed_checks = build_checks(mixed_remote)
    old_only_checks = build_checks(old_only_remote)
    mixed_empty_checks = build_empty_checks(mixed_remote)
    old_only_empty_checks = build_empty_checks(old_only_remote)

    # Baseline: run all checks from the first old node.
    mixed_baseline = run_checks(old_node_1, mixed_checks)

    # Verify all other nodes produce the same results.
    for node in [old_node_2, new_node_1, new_node_2]:
        results = run_checks(node, mixed_checks)
        for name in mixed_baseline:
            assert results[name] == mixed_baseline[name], f"{name} on {node.name}"

    # Old-only baseline (using same 2 old nodes twice) — must match mixed baseline.
    old_only_baseline = run_checks(old_node_1, old_only_checks)
    for name in mixed_baseline:
        assert old_only_baseline[name] == mixed_baseline[name], name

    # Cover empty aggregate states.
    mixed_empty_baseline = run_checks(old_node_1, mixed_empty_checks)
    for node in [old_node_2, new_node_1, new_node_2]:
        results = run_checks(node, mixed_empty_checks)
        for name in mixed_empty_baseline:
            assert results[name] == mixed_empty_baseline[name], f"{name} on {node.name}"

    old_only_empty_baseline = run_checks(old_node_1, old_only_empty_checks)
    for name in mixed_empty_baseline:
        assert old_only_empty_baseline[name] == mixed_empty_baseline[name], name

    # --- Cross-version state reading via AggregatingMergeTree ---
    agg_table_ddl = (
        "CREATE TABLE agg_states (s AggregateFunction(cramersV, UInt8, UInt8)) "
        "ENGINE = AggregatingMergeTree ORDER BY tuple()"
    )
    insert_agg = (
        "INSERT INTO agg_states "
        "SELECT cramersVState(x, y) FROM tab_contingency"
    )
    read_agg = "SELECT roundBankers(cramersVMerge(s), 4) FROM agg_states"

    # Direction 1: old agg -> new agg
    old_node_1.query(agg_table_ddl)
    old_node_1.query(insert_agg)
    old_result = old_node_1.query(read_agg).strip()

    new_node_1.query(agg_table_ddl)
    new_node_1.query(
        f"INSERT INTO agg_states SELECT * FROM remote('old_node_1', default, agg_states)"
    )
    assert new_node_1.query(read_agg).strip() == old_result, "old agg -> new agg mismatch"

    # Direction 2: new agg -> old agg
    new_node_2.query(agg_table_ddl)
    new_node_2.query(insert_agg)
    assert new_node_2.query(read_agg).strip() == old_result, "new agg result mismatch"

    old_node_2.query(agg_table_ddl)
    old_node_2.query(
        f"INSERT INTO agg_states SELECT * FROM remote('new_node_2', default, agg_states)"
    )
    assert old_node_2.query(read_agg).strip() == old_result, "new agg -> old agg mismatch"

    # Direction 3: new window -> old agg
    new_node_1.query("TRUNCATE TABLE agg_states")
    new_node_1.query(
        "INSERT INTO agg_states "
        "SELECT s FROM ("
        "  SELECT cramersVState(x, y) OVER () AS s FROM tab_contingency"
        "  LIMIT 1"
        ")"
    )
    assert new_node_1.query(read_agg).strip() == old_result, "new window result mismatch"

    old_node_1.query("TRUNCATE TABLE agg_states")
    old_node_1.query(
        f"INSERT INTO agg_states SELECT * FROM remote('new_node_1', default, agg_states)"
    )
    assert old_node_1.query(read_agg).strip() == old_result, "new window -> old agg mismatch"

    # Direction 4: new window -> new agg (same binary, cross-variant)
    new_node_2.query("TRUNCATE TABLE agg_states")
    new_node_2.query(
        f"INSERT INTO agg_states SELECT * FROM remote('new_node_1', default, agg_states)"
    )
    assert new_node_2.query(read_agg).strip() == old_result, "new window -> new agg mismatch"

    # --- Upgrade one old node and re-check ---
    old_node_1.restart_with_latest_version(fix_metadata=True)

    for node in all_nodes:
        results = run_checks(node, mixed_checks)
        for name in mixed_baseline:
            assert results[name] == mixed_baseline[name], f"{name} on {node.name} (after upgrade)"

        results = run_checks(node, mixed_empty_checks)
        for name in mixed_empty_baseline:
            assert results[name] == mixed_empty_baseline[name], f"{name} on {node.name} (after upgrade)"

    # Cleanup
    for node in all_nodes:
        node.query("DROP TABLE IF EXISTS tab_contingency")
        node.query("DROP TABLE IF EXISTS agg_states")
