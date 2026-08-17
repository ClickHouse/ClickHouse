import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

ROWS_PER_PART = 1000
TOTAL_ROWS = 2 * ROWS_PER_PART

# The wrong answer needs the direct-read rewrite to run and the skip index to be applied while
# reading data; asserting ground truth cannot detect a default flip, so every asserting query
# pins these explicitly.
ARMING_SETTINGS = (
    "use_skip_indexes = 1, "
    "use_skip_indexes_on_data_read = 1, "
    "query_plan_direct_read_from_text_index = 1"
)

# `EXPLAIN indexes = 1` reports granule counts from analysis, which only happens when the index
# is not applied while reading data. A query-level setting is applied after EXPLAIN forces that
# off internally, so asking for data-read-time application here would report every granule as
# read and the pruning assertion could never fail.
EXPLAIN_SETTINGS = (
    "use_skip_indexes = 1, "
    "use_skip_indexes_on_data_read = 0, "
    "query_plan_direct_read_from_text_index = 1"
)


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


def stop_merges(table):
    # Redundant second line of defence. The guard that actually holds is
    # `max_bytes_to_merge_at_max_space_in_pool = 0` in the table metadata: this statement locks
    # the storage instance, and `ATTACH TABLE` runs `startup` on a fresh instance before
    # returning, so a merge can be selected before any later statement reaches the server.
    node.query(f"SYSTEM STOP MERGES {table}")


def assert_two_parts(table):
    parts = node.query(
        f"SELECT groupArray(name) FROM ("
        f"  SELECT name FROM system.parts "
        f"  WHERE database = currentDatabase() AND table = '{table}' AND active "
        f"  ORDER BY name)"
    ).strip()
    assert parts == "['all_1_1_0','all_2_2_0']", f"part layout changed: {parts}"


def make_partially_materialized(table, packed_max_bytes=0):
    # The first part predates the text index, so `tx` is materialized only in the second.
    # A partially materialized index is what arms the direct-read optimization while still
    # requiring the first part to be read through the virtual column's default expression.
    # `mm_k` gives the first part a skip-index file to copy from; the remaining settings keep
    # index filenames and sizes predictable. A merge would rebuild the unaccounted index and
    # repair the very shape these arms assert on, so merges are disabled in table metadata,
    # which is checked before any merge selector runs and applies on every startup.
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, s String, INDEX mm_k k TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
                 packed_skip_index_max_bytes = {packed_max_bytes},
                 index_granularity = 100,
                 replace_long_file_name_to_hash = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0,
                 max_bytes_to_merge_at_max_space_in_pool = 0
        """
    )
    stop_merges(table)
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


def reattach(table):
    # `DETACH` must be `SYNC`: an asynchronous detach leaves the instance tracked in
    # `DatabaseAtomic::detached_tables` while another subsystem still holds a `StoragePtr`, and
    # the following `ATTACH` then throws `TABLE_ALREADY_EXISTS` rather than waiting.
    node.query(f"DETACH TABLE {table} SYNC")
    node.query(f"ATTACH TABLE {table}")
    # The merge guard lives in table metadata so that it is already in force during the startup
    # `ATTACH` performs before it returns; assert it survived rather than assuming it.
    engine_full = scalar(
        f"SELECT engine_full FROM system.tables "
        f"WHERE database = currentDatabase() AND name = '{table}'"
    )
    assert "max_bytes_to_merge_at_max_space_in_pool = 0" in engine_full, (
        f"merge guard missing from table metadata after reattach: {engine_full}"
    )
    stop_merges(table)


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

    reattach(table)


def scalar(query):
    return node.query(query).strip()


def test_orphan_index_file_does_not_drop_rows(started_cluster):
    # An unaccounted index file used to make the read path treat the part as indexed while no
    # index reader was assigned to it, so the part contributed no matches at all.
    table = "orphan_read"
    make_partially_materialized(table)
    inject_orphan_index_file(table)
    assert_two_parts(table)

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
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
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
            f"  SETTINGS {ARMING_SETTINGS}, query_plan_optimize_count_from_text_index = 0)"
        )
        == f"[('all_1_1_0',{ROWS_PER_PART}),('all_2_2_0',{ROWS_PER_PART})]"
    )
    # A row-returning query reads the same columns through the same injection path.
    assert (
        scalar(
            f"SELECT count() FROM (SELECT k, s FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS})"
        )
        == str(TOTAL_ROWS)
    )


def test_no_orphan_is_unaffected(started_cluster):
    # Guards the fixture: the same table shape without the injected file must already be
    # correct, so a failure above cannot be blamed on the partially materialized index alone.
    table = "orphan_read_control"
    make_partially_materialized(table)
    reattach(table)
    assert_two_parts(table)

    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == "0"
    )


def test_index_materialized_in_no_part(started_cluster):
    # An index materialized in no part disables the direct-read rewrite entirely, so no virtual
    # column is created and the predicate answers through the ordinary row-level path.
    # The fixture inserts every row in one statement, so this arm has a single part and asserts
    # only table-level counts; a two-part layout guard would never hold here.
    table = "orphan_read_unmaterialized"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, s String) ENGINE = MergeTree ORDER BY k
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100,
                 max_bytes_to_merge_at_max_space_in_pool = 0
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
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
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
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100,
                 max_bytes_to_merge_at_max_space_in_pool = 0
        """
    )
    stop_merges(table)
    node.query(
        f"INSERT INTO {table} SELECT number, if(number < 400, 'alpha beta', 'gamma delta') "
        f"FROM numbers({ROWS_PER_PART})"
    )
    node.query(
        f"INSERT INTO {table} SELECT number + {ROWS_PER_PART}, "
        f"if(number < 200, 'alpha beta', 'gamma delta') FROM numbers({ROWS_PER_PART})"
    )
    assert_two_parts(table)

    expected = scalar(
        f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') SETTINGS use_skip_indexes = 0"
    )
    assert expected == "600"
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == expected
    )
    # The count above is the correct answer whether it came from the index or from the base
    # column, so it cannot see the predicate refusing a materialized part. Reading the match
    # from the index reads one byte per row of the virtual column, while falling back reads the
    # whole string column, so the bytes read distinguish the two routes.
    def bytes_read(direct_read):
        log_comment = f"{table}_bytes_{direct_read}"
        node.query(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, "
            f"query_plan_direct_read_from_text_index = {direct_read}, "
            f"query_plan_optimize_count_from_text_index = 0, log_comment = '{log_comment}'"
        )
        node.query("SYSTEM FLUSH LOGS query_log")
        return int(
            scalar(
                f"SELECT ProfileEvents['SelectedBytes'] FROM system.query_log "
                f"WHERE type = 'QueryFinish' AND current_database = currentDatabase() "
                f"AND log_comment = '{log_comment}' "
                f"ORDER BY event_time_microseconds DESC LIMIT 1"
            )
        )

    from_index = bytes_read(1)
    from_column = bytes_read(0)
    assert from_index < from_column, (
        f"the predicate refused a materialized part: {from_index} bytes read with direct read "
        f"from the index, {from_column} without it"
    )

    assert (
        scalar(
            f"SELECT count() > 0 FROM ("
            f"  EXPLAIN indexes = 1 SELECT * FROM {table} WHERE hasToken(s, 'alpha') "
            f"  SETTINGS {EXPLAIN_SETTINGS}) "
            f"WHERE explain ILIKE '%Name: tx%'"
        )
        == "1"
    )
    # The index must actually drop granules, otherwise the arm above would pass on a plan that
    # names the index and then reads everything. Read the reader's own granule count from the
    # `Parts: N | Granules: M` line: the per-index `Granules: M/N` lines include the primary
    # key's, which never prunes here.
    def granules_read(settings):
        return int(
            scalar(
                f"SELECT extract(explain, 'Granules: (\\\\d+)$') FROM ("
                f"  EXPLAIN indexes = 1 SELECT * FROM {table} WHERE hasToken(s, 'alpha') "
                f"  SETTINGS {settings}) "
                f"WHERE explain ILIKE '%Parts:%|%Granules:%'"
            )
        )

    with_index = granules_read(EXPLAIN_SETTINGS)
    without_index = granules_read("use_skip_indexes = 0")
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
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100,
                 max_bytes_to_merge_at_max_space_in_pool = 0
        """
    )
    stop_merges(table)
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
    assert_two_parts(table)

    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == "600"
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == "1400"
    )
    assert (
        scalar(
            f"SELECT groupArray((_part, c)) FROM ("
            f"  SELECT _part, count() AS c FROM {table} WHERE hasToken(s, 'alpha') "
            f"  GROUP BY _part ORDER BY _part "
            f"  SETTINGS {ARMING_SETTINGS}, query_plan_optimize_count_from_text_index = 0)"
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
                 columns_and_secondary_indices_sizes_lazy_calculation = 0,
                 max_bytes_to_merge_at_max_space_in_pool = 0
        """
    )
    stop_merges(table)
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
    assert_two_parts(table)

    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == str(TOTAL_ROWS)
    )
    assert (
        scalar(
            f"SELECT count() FROM {table} WHERE NOT hasToken(s, 'alpha') "
            f"SETTINGS {ARMING_SETTINGS}"
        )
        == "0"
    )
