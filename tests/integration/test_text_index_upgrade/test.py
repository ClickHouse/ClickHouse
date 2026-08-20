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


SEARCH_SETTINGS = {
    # Exercise the lazy posting list apply mode against the upgraded binary:
    # pre-WithCodec granules silently fall back to eager mode, while the new-format
    # part inserted after the upgrade actually uses the cursor-based reader.
    "text_index_posting_list_apply_mode": "lazy",
    # Keep count() on the index-scan plan (Name: idx), not the count-from-index rewrite.
    "query_plan_optimize_count_from_text_index": 0,
}


def run_search_queries(node, table, settings=None):
    return [
        node.query(q.format(table=table), settings=settings).strip()
        for q, _ in SEARCH_QUERIES
    ]


def expected_results():
    return [expected for _, expected in SEARCH_QUERIES]


# The count-from-index rewrite (default on) applies to a bare count() with a
# text predicate; the arraySort(groupArray(...)) query is not a count.
COUNT_QUERIES = [q for q, _ in SEARCH_QUERIES if q.lstrip().startswith("SELECT count()")]


COUNT_OPTIMIZATION_SETTINGS = {
    "query_plan_optimize_count_from_text_index": 1,
    "query_plan_direct_read_from_text_index": 1,
    "optimize_trivial_count_query": 1,
}


def assert_count_from_index_agrees(node, table):
    # The rewrite answers count() from posting/dictionary cardinalities instead
    # of scanning rows. Prove it agrees with the index-scan count on whatever
    # parts the table currently holds, so its decode is checked against
    # pre-WithCodec and mixed-format segments, not only new-format ones.

    # Fail loud if the rewrite is not engaged, otherwise the checks below pass
    # vacuously (both paths would just be the index scan).
    plan = node.query(
        ("EXPLAIN " + COUNT_QUERIES[0]).format(table=table),
        settings=COUNT_OPTIMIZATION_SETTINGS,
    )
    assert "ReadFromTextIndexCount" in plan, (
        f"count-from-index rewrite not engaged after upgrade:\n{plan}"
    )

    for q in COUNT_QUERIES:
        optimized = node.query(
            q.format(table=table),
            settings=COUNT_OPTIMIZATION_SETTINGS,
        ).strip()
        reader = node.query(
            q.format(table=table),
            settings={"query_plan_optimize_count_from_text_index": 0},
        ).strip()
        assert optimized == reader, (
            f"count-from-index disagrees with the index scan for `{q}`: "
            f"optimized={optimized} reader={reader}"
        )


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
    # Lazy mode falls back to materialize for these pre-WithCodec granules.
    assert run_search_queries(node, table, settings=SEARCH_SETTINGS) == expected_results()

    # Confirm the text index is engaged after upgrade; without this check a
    # silent fallback to full scan would still pass the queries above.
    explain = node.query(
        f"EXPLAIN indexes = 1 "
        f"SELECT count() FROM {table} WHERE hasToken(s, 'unique42')",
        settings=SEARCH_SETTINGS,
    )
    assert "Name: idx" in explain, (
        f"text index `idx` not picked up after upgrade:\n{explain}"
    )

    # count-from-index must decode the pre-WithCodec segments on its own path.
    assert_count_from_index_agrees(node, table)

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
    # Mixed run: old-format parts take the materialize fallback; the new-format
    # part can satisfy the lazy-mode preconditions.
    assert run_search_queries(node, table, settings=SEARCH_SETTINGS) == mixed_expected

    # count-from-index across mixed old- and new-format parts in one query.
    assert_count_from_index_agrees(node, table)

    # Confirm the new-format part is indexed for the new token.
    assert (
        node.query(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'unique5042')",
            settings=SEARCH_SETTINGS,
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
    # was correctly merged, not just readable. The merged part is new-format,
    # so lazy mode is now actually engaged for every query.
    assert run_search_queries(node, table, settings=SEARCH_SETTINGS) == mixed_expected
    assert (
        node.query(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'unique5042')",
            settings=SEARCH_SETTINGS,
        ).strip()
        == "1"
    )

    # count-from-index on the merged new-format part built from mixed segments.
    assert_count_from_index_agrees(node, table)

    node.query(f"DROP TABLE {table} SYNC")
    node.restart_with_original_version()
