import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

ROWS_PER_PART = 1000
TOTAL_ROWS = 2 * ROWS_PER_PART


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def part_path(table, part_name):
    return node.query(
        f"SELECT path FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' "
        f"AND name = '{part_name}' AND active"
    ).strip()


def make_partially_materialized(table, packed_max_bytes=0):
    # The first part predates the text index, so `tx` is materialized only in the second.
    # A partially materialized index is what arms the direct-read optimization while still
    # requiring the first part to be read through the virtual column's default expression.
    # `mm_k` gives the first part a skip-index file to copy from; the remaining settings keep
    # index filenames and sizes predictable.
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, s String, INDEX mm_k k TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 packed_skip_index_max_bytes = {packed_max_bytes},
                 index_granularity = 100,
                 replace_long_file_name_to_hash = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, 'alpha beta' FROM numbers({ROWS_PER_PART})"
    )
    node.query(
        f"ALTER TABLE {table} ADD INDEX tx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1"
    )
    node.query(
        f"INSERT INTO {table} SELECT number + {ROWS_PER_PART}, 'alpha beta' "
        f"FROM numbers({ROWS_PER_PART})"
    )


def inject_orphan_index_file(table, part_name="all_1_1_0"):
    # Reproduces the corrupted-part shape of issue #109595: a skp_idx_tx.idx that exists on
    # disk while checksums.txt does not account for it. The bytes are borrowed from the minmax
    # index so the file is non-empty; its content is never decoded.
    path = part_path(table, part_name)
    source = node.exec_in_container(
        ["bash", "-c", f"ls {path} | grep -E '^skp_idx_mm_k\\.idx' | head -1"],
        privileged=True,
    ).strip()
    assert source, f"no minmax index file to copy from in {path}"

    node.exec_in_container(
        ["bash", "-c", f"head -c 512 {path}/{source} > {path}/skp_idx_tx.idx"],
        privileged=True,
    )
    size = node.exec_in_container(
        ["bash", "-c", f"stat -c%s {path}/skp_idx_tx.idx"], privileged=True
    ).strip()
    assert int(size) > 0, "orphan index file is empty"

    accounted = node.exec_in_container(
        ["bash", "-c", f"grep -c skp_idx_tx {path}/checksums.txt || true"],
        privileged=True,
    ).strip()
    assert accounted == "0", "orphan index file must not be accounted in checksums.txt"

    node.query(f"DETACH TABLE {table}")
    node.query(f"ATTACH TABLE {table}")


def scalar(query):
    return node.query(query).strip()


def test_orphan_index_file_does_not_drop_rows(started_cluster):
    # An unaccounted index file used to make the read path treat the part as indexed while no
    # index reader was assigned to it, so the part contributed no matches at all.
    table = "orphan_read"
    make_partially_materialized(table)
    inject_orphan_index_file(table)

    assert scalar(f"SELECT count() FROM {table}") == str(TOTAL_ROWS)
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 0"
        )
        == str(TOTAL_ROWS)
    )
    # Every row matches, so the predicate must answer with the full row count and its negation
    # with zero. Before the fix these answered ROWS_PER_PART and ROWS_PER_PART.
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == "0"
    )
    # Both parts must contribute; the aggregate above would also pass if one part returned
    # every row twice.
    assert (
        scalar(
            f"SELECT groupArray((_part, c)) FROM ("
            f"  SELECT _part, count() AS c FROM {table} WHERE hasToken(s, 'alpha') "
            f"  GROUP BY _part ORDER BY _part "
            f"  SETTINGS use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 0)"
        )
        == f"[('all_1_1_0',{ROWS_PER_PART}),('all_2_2_0',{ROWS_PER_PART})]"
    )
    # A row-returning query reads the same columns through the same injection path.
    assert (
        scalar(
            f"SELECT count() FROM (SELECT k, s FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1)"
        )
        == str(TOTAL_ROWS)
    )


def test_no_orphan_is_unaffected(started_cluster):
    # Guards the fixture: the same table shape without the injected file must already be
    # correct, so a failure above cannot be blamed on the partially materialized index alone.
    table = "orphan_read_control"
    make_partially_materialized(table)
    node.query(f"DETACH TABLE {table}")
    node.query(f"ATTACH TABLE {table}")

    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == "0"
    )


def test_index_materialized_in_no_part(started_cluster):
    # The index name is absent from the read tasks, which is the lookup-miss path of the read
    # predicate. It must fall back to reading the base column rather than claim the index.
    table = "orphan_read_unmaterialized"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, s String) ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, 'alpha beta' FROM numbers({TOTAL_ROWS})"
    )
    node.query(
        f"ALTER TABLE {table} ADD INDEX tx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1"
    )

    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == "0"
    )


def test_fully_materialized_index_still_prunes(started_cluster):
    # Anti-regression: a stricter read predicate must not silently disable the optimization.
    # Granule pruning is asserted structurally, not by timing.
    table = "orphan_read_materialized"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table}
        (k UInt64, s String, INDEX tx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, if(number < 400, 'alpha beta', 'gamma delta') "
        f"FROM numbers({ROWS_PER_PART})"
    )
    node.query(
        f"INSERT INTO {table} SELECT number + {ROWS_PER_PART}, "
        f"if(number < 200, 'alpha beta', 'gamma delta') FROM numbers({ROWS_PER_PART})"
    )

    expected = scalar(
        f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') SETTINGS use_skip_indexes = 0"
    )
    assert expected == "600"
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == expected
    )
    assert (
        scalar(
            f"SELECT count() > 0 FROM ("
            f"  EXPLAIN indexes = 1 SELECT * FROM {table} WHERE hasToken(s, 'alpha') "
            f"  SETTINGS use_skip_indexes = 1) "
            f"WHERE explain ILIKE '%Name: tx%'"
        )
        == "1"
    )
    # The index must actually drop granules, otherwise the arm above would pass on a plan that
    # names the index and then reads everything. Read the reader's own granule count from the
    # `Parts: N | Granules: M` line: the per-index `Granules: M/N` lines include the primary
    # key's, which never prunes here.
    def granules_read(use_skip_indexes):
        return int(
            scalar(
                f"SELECT extract(explain, 'Granules: (\\\\d+)$') FROM ("
                f"  EXPLAIN indexes = 1 SELECT * FROM {table} WHERE hasToken(s, 'alpha') "
                f"  SETTINGS use_skip_indexes = {use_skip_indexes}) "
                f"WHERE explain ILIKE '%Parts:%|%Granules:%'"
            )
        )

    with_index = granules_read(1)
    without_index = granules_read(0)
    assert with_index < without_index, (
        f"index did not prune granules: {with_index} read with the index, "
        f"{without_index} without it"
    )


def test_partially_materialized_index_without_corruption(started_cluster):
    # The ordinary partially materialized shape, with no injected file: the unindexed part is
    # read through the default expression. This is the behaviour the fix routes the orphan case
    # to, so it must keep answering ground truth.
    table = "orphan_read_partial"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, s String) ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, if(number < 400, 'alpha beta', 'gamma delta') "
        f"FROM numbers({ROWS_PER_PART})"
    )
    node.query(
        f"ALTER TABLE {table} ADD INDEX tx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1"
    )
    node.query(
        f"INSERT INTO {table} SELECT number + {ROWS_PER_PART}, "
        f"if(number < 200, 'alpha beta', 'gamma delta') FROM numbers({ROWS_PER_PART})"
    )

    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == "600"
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == "1400"
    )
    assert (
        scalar(
            f"SELECT groupArray((_part, c)) FROM ("
            f"  SELECT _part, count() AS c FROM {table} WHERE hasToken(s, 'alpha') "
            f"  GROUP BY _part ORDER BY _part "
            f"  SETTINGS use_skip_indexes = 1, query_plan_optimize_count_from_text_index = 0)"
        )
        == "[('all_1_1_0',400),('all_2_2_0',200)]"
    )


def test_orphan_index_file_on_compact_part(started_cluster):
    # Part type is not a dimension of the defect; the predicate is about file accounting.
    table = "orphan_read_compact"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, s String, INDEX mm_k k TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS packed_skip_index_max_bytes = 0, index_granularity = 100,
                 replace_long_file_name_to_hash = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )
    node.query(
        f"INSERT INTO {table} SELECT number, 'alpha beta' FROM numbers({ROWS_PER_PART})"
    )
    node.query(
        f"ALTER TABLE {table} ADD INDEX tx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1"
    )
    node.query(
        f"INSERT INTO {table} SELECT number + {ROWS_PER_PART}, 'alpha beta' "
        f"FROM numbers({ROWS_PER_PART})"
    )
    assert (
        node.query(
            f"SELECT part_type FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND name = 'all_1_1_0' AND active"
        ).strip()
        == "Compact"
    )
    inject_orphan_index_file(table)

    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1"
        )
        == "0"
    )
