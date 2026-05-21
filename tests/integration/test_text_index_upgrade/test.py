import pytest

from helpers.cluster import ClickHouseCluster

# 26.4 writes the pre-WithCodec header format; the new reader recovers the
# posting list codec from the index DDL. We test both the default codec and
# 'bitpacking', where DDL recovery is the only way to decode old segments.
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


# Queries with known answers covering single-token lookups, AND intersection,
# OR union, and a missing token.
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

    # Small posting_list_block_size makes each posting list span many packed
    # blocks, exercising the on-disk layout walked by the new reader.
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

    # 5000 rows: every row carries 'shared'; even k -> 'common', odd k -> 'rare';
    # row 42 carries 'unique42'. Inserted as two parts then merged so the
    # resulting posting lists are stitched from multiple segments.
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

    # Ground truth from the old server: pre-WithCodec layout on disk, old reader.
    assert run_search_queries(node, table) == expected_results()

    # Swap the binary but keep the data dir; the new reader must load the
    # old-format index segments produced above.
    node.restart_with_latest_version()

    # Same data, same queries, same answers under the upgraded binary.
    assert run_search_queries(node, table) == expected_results()

    # Confirm the text index is engaged after upgrade; without this check a
    # silent fallback to full scan would still pass the queries above.
    explain = node.query(
        f"EXPLAIN indexes = 1 "
        f"SELECT count() FROM {table} WHERE hasToken(s, 'unique42')"
    )
    assert "Name: idx" in explain, (
        f"text index `idx` not picked up after upgrade:\n{explain}"
    )

    # Insert a third part via the upgraded binary so the table has both old-
    # and new-format index segments side-by-side.
    node.query(
        f"""
        INSERT INTO {table}
        SELECT
            number + 5000,
            concat(
                'shared ',
                if(number % 2 = 0, 'common', 'rare'),
                if(number = 42, ' unique5042', '')
            )
        FROM numbers(2500)
        """
    )

    # After the new insert 'common' and 'rare' each gain 1250 rows; 'unique42'
    # stays at row 42; 'unique5042' is new on row 5042 in the new-format part.
    mixed_expected = [
        "3750",  # hasToken 'common'
        "3750",  # hasToken 'rare'
        "1",     # hasToken 'unique42'
        "0",     # hasToken 'absent'
        "3750",  # hasAllTokens ['common', 'shared']
        "3751",  # hasAnyTokens ['rare', 'unique42']
        "[42]",  # arraySort(groupArray(k)) for 'unique42'
    ]
    assert run_search_queries(node, table) == mixed_expected

    # Confirm the new-format part is indexed for the new token.
    assert (
        node.query(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'unique5042')"
        ).strip()
        == "1"
    )

    # Merge across mixed-format parts: posting lists from the old pre-WithCodec
    # layout and the new layout are read back and re-emitted as one new-format
    # part. Fails if the new reader cannot decode old segments end-to-end.
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    # Exactly one active part proves the merge actually ran (a silent skip
    # would still answer the queries below correctly).
    active_parts = node.query(
        f"SELECT count() FROM system.parts "
        f"WHERE table = '{table}' AND active"
    ).strip()
    assert active_parts == "1", (
        f"expected a single active part after OPTIMIZE FINAL, got {active_parts}"
    )

    # Same queries against the merged part: checks mixed-version index data
    # was correctly merged, not just readable.
    assert run_search_queries(node, table) == mixed_expected
    assert (
        node.query(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'unique5042')"
        ).strip()
        == "1"
    )

    node.query(f"DROP TABLE {table} SYNC")
    node.restart_with_original_version()
