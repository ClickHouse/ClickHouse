import pytest

from helpers.cluster import ClickHouseCluster

# Text indexes written by 26.4 use the pre-WithCodec header format, which does
# not persist the posting list codec type. The newer reader recovers it from
# the index DDL definition. This test exercises both:
#   * the default codec ("none"), where the recovery path is a no-op
#   * "bitpacking", where the recovery path is the only way the reader can
#     decode old segments
OLD_VERSION_TAG = "26.4"


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node",
            image="clickhouse/clickhouse-server",
            tag=OLD_VERSION_TAG,
            with_installed_binary=True,
            stay_alive=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# A handful of queries with known answers. They are intentionally varied so
# that they exercise:
#   * single-token lookups (hasToken)
#   * AND intersection (hasAllTokens)
#   * OR union (hasAnyTokens)
#   * a token that does not exist
SEARCH_QUERIES = [
    ("SELECT count() FROM {table} WHERE hasToken(s, 'common')", "2500"),
    ("SELECT count() FROM {table} WHERE hasToken(s, 'rare')", "2500"),
    ("SELECT count() FROM {table} WHERE hasToken(s, 'unique42')", "1"),
    ("SELECT count() FROM {table} WHERE hasToken(s, 'absent')", "0"),
    (
        "SELECT count() FROM {table} WHERE hasAllTokens(s, ['common', 'shared'])",
        "2500",
    ),
    (
        "SELECT count() FROM {table} WHERE hasAnyTokens(s, ['rare', 'unique42'])",
        "2501",
    ),
    (
        "SELECT arraySort(groupArray(k)) FROM {table} "
        "WHERE hasToken(s, 'unique42')",
        "[42]",
    ),
]


def create_and_populate(node, table, posting_list_codec):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")

    codec_clause = ""
    if posting_list_codec is not None:
        codec_clause = f", posting_list_codec = '{posting_list_codec}'"

    # A small posting_list_block_size makes each posting list span many
    # packed blocks, exercising the on-disk layout that the new reader has
    # to walk over for old-format indexes.
    node.query(
        f"""
        CREATE TABLE {table} (
            k UInt64,
            s String,
            INDEX idx s TYPE text(
                tokenizer = 'splitByNonAlpha',
                posting_list_block_size = 64
                {codec_clause}
            )
        )
        ENGINE = MergeTree
        ORDER BY k
        SETTINGS index_granularity = 128
        """
    )

    # Build a corpus of 5000 rows where:
    #   * every row carries the token 'shared'
    #   * 'common' (even k) vs 'rare' (odd k)
    #   * row 42 has a unique token 'unique42'
    # Insert as two parts and then merge so the resulting part's posting
    # lists are stitched together from multiple segments — the part of the
    # on-disk format that the new reader has to walk for old-format indexes.
    node.query(
        f"""
        INSERT INTO {table}
        SELECT
            number,
            concat(
                'shared ',
                if(number % 2 = 0, 'common', 'rare'),
                if(number = 42, ' unique42', '')
            )
        FROM numbers(2500)
        """
    )
    node.query(
        f"""
        INSERT INTO {table}
        SELECT
            number + 2500,
            concat('shared ', if(number % 2 = 0, 'common', 'rare'))
        FROM numbers(2500)
        """
    )

    node.query(f"OPTIMIZE TABLE {table} FINAL")


def run_search_queries(node, table):
    return [node.query(q.format(table=table)).strip() for q, _ in SEARCH_QUERIES]


def expected_results():
    return [expected for _, expected in SEARCH_QUERIES]


@pytest.mark.parametrize(
    "posting_list_codec",
    [
        pytest.param(None, id="default_codec"),
        pytest.param("bitpacking", id="bitpacking_codec"),
    ],
)
def test_text_index_upgrade(started_cluster, posting_list_codec):
    node = started_cluster.instances["node"]
    table = f"text_index_upgrade_{posting_list_codec or 'default'}"

    create_and_populate(node, table, posting_list_codec)

    # Ground truth produced by the old server: the index format on disk is the
    # pre-WithCodec layout, and the queries below are answered by the old
    # reader.
    assert run_search_queries(node, table) == expected_results()

    # Swap the binary in place but keep the data directory. After the
    # restart the new reader must be able to load and use the old-format
    # index segments produced above.
    node.restart_with_latest_version()

    # Re-run the same queries against the upgraded binary. Same data, same
    # queries, same answers.
    assert run_search_queries(node, table) == expected_results()

    # Sanity check: the index is actually being used after the upgrade. If
    # the reader silently fell back to a full scan the queries above would
    # still pass, so we check the explain plan to make sure the text index
    # is engaged.
    explain = node.query(
        f"EXPLAIN indexes = 1 "
        f"SELECT count() FROM {table} WHERE hasToken(s, 'unique42')"
    )
    assert "Name: idx" in explain, (
        f"text index `idx` not picked up after upgrade:\n{explain}"
    )

    node.query(f"DROP TABLE {table} SYNC")
    node.restart_with_original_version()
