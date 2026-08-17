import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

# A background merge would rebuild the unaccounted index and repair the shape under test.
NO_MERGES = "max_bytes_to_merge_at_max_space_in_pool = 0"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def q(query):
    return node.query(query).strip()


def test_orphan_index_file_does_not_drop_rows(started_cluster):
    q("DROP TABLE IF EXISTS t SYNC")
    q(f"CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS {NO_MERGES}")
    q("INSERT INTO t VALUES (1, 'alpha')")
    q("ALTER TABLE t ADD INDEX tx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1")
    q("INSERT INTO t VALUES (2, 'alpha')")

    # An index file that exists on disk while checksums.txt does not account for it, as in #109595.
    # Its content is never decoded.
    path = q(
        "SELECT path FROM system.parts WHERE database = currentDatabase() "
        "AND table = 't' AND name = 'all_1_1_0' AND active"
    )
    node.exec_in_container(
        ["bash", "-c", f"echo garbage > {path}/skp_idx_tx.idx"], privileged=True
    )
    assert (
        node.exec_in_container(
            ["bash", "-c", f"grep -c skp_idx_tx {path}/checksums.txt || true"],
            privileged=True,
        ).strip()
        == "0"
    )
    q("DETACH TABLE t SYNC")
    q("ATTACH TABLE t")

    assert q("SELECT count() FROM t") == "2"
    # Both rows match. Before the fix these answered 1 and 1.
    assert q("SELECT count() FROM t WHERE hasToken(s, 'alpha')") == "2"
    assert q("SELECT count() FROM t WHERE NOT hasToken(s, 'alpha')") == "0"


def test_materialized_index_is_still_read_from_index(started_cluster):
    # A stricter read predicate must not stop using an index that is materialized.
    q("DROP TABLE IF EXISTS m SYNC")
    q(
        "CREATE TABLE m (k UInt64, s String, "
        "INDEX tx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1) "
        f"ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100, {NO_MERGES}"
    )
    q("INSERT INTO m SELECT number, if(number < 400, 'alpha beta', 'gamma delta') FROM numbers(1000)")

    assert q("SELECT count() FROM m WHERE hasToken(s, 'alpha')") == "400"

    # That count is correct whether it came from the index or from the base column, so it cannot
    # see the predicate refusing the part. Reading the match from the index reads one byte per row;
    # falling back reads the whole string column, so the bytes read distinguish the two routes.
    def bytes_read(direct_read):
        tag = f"m_bytes_{direct_read}"
        q(
            "SELECT count() FROM m WHERE hasToken(s, 'alpha') SETTINGS "
            f"query_plan_direct_read_from_text_index = {direct_read}, "
            f"query_plan_optimize_count_from_text_index = 0, log_comment = '{tag}'"
        )
        q("SYSTEM FLUSH LOGS query_log")
        return int(
            q(
                "SELECT ProfileEvents['SelectedBytes'] FROM system.query_log "
                "WHERE type = 'QueryFinish' AND current_database = currentDatabase() "
                f"AND log_comment = '{tag}' ORDER BY event_time_microseconds DESC LIMIT 1"
            )
        )

    from_index = bytes_read(1)
    from_column = bytes_read(0)
    assert from_index < from_column, f"index not used: {from_index} vs {from_column} bytes"
