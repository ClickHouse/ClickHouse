import pytest

from helpers.cluster import ClickHouseCluster

# 26.4 writes the pre-V1_WithCodec header format; the new reader recovers the
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
        # Same as `node`, but its default profile pins `compatibility` to a pre-26.6
        # version. After the upgrade this makes the new binary resolve
        # `text_index_serialization_version` to `v0_initial` on its own, without persisting any setting
        # into the table metadata, which is the realistic rolling-upgrade knob.
        cluster.add_instance(
            "node_compat",
            image="clickhouse/clickhouse-server",
            tag=OLD_VERSION_TAG,
            with_installed_binary=True,
            stay_alive=True,
            user_configs=["configs/compatibility.xml"],
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


# Exercise the lazy posting list apply mode against the upgraded binary:
# pre-V1_WithCodec granules silently fall back to eager mode, while the new-format
# part inserted after the upgrade actually uses the cursor-based reader.
LAZY_APPLY_SETTINGS = {
    "text_index_posting_list_apply_mode": "lazy",
}


def run_search_queries(node, table, settings=None):
    return [
        node.query(q.format(table=table), settings=settings).strip()
        for q, _ in SEARCH_QUERIES
    ]


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

    # Ground truth from the old server: pre-V1_WithCodec layout on disk, old reader.
    assert run_search_queries(node, table) == expected_results()

    # Swap the binary but keep the data dir; the new reader must load the
    # old-format index segments produced above.
    node.restart_with_latest_version()

    # Same data, same queries, same answers under the upgraded binary.
    # Lazy mode falls back to materialize for these pre-V1_WithCodec granules.
    assert run_search_queries(node, table, settings=LAZY_APPLY_SETTINGS) == expected_results()

    # Confirm the text index is engaged after upgrade; without this check a
    # silent fallback to full scan would still pass the queries above.
    explain = node.query(
        f"EXPLAIN indexes = 1 "
        f"SELECT count() FROM {table} WHERE hasToken(s, 'unique42')",
        settings=LAZY_APPLY_SETTINGS,
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
    # Mixed run: old-format parts take the materialize fallback; the new-format
    # part can satisfy the lazy-mode preconditions.
    assert run_search_queries(node, table, settings=LAZY_APPLY_SETTINGS) == mixed_expected

    # Confirm the new-format part is indexed for the new token.
    assert (
        node.query(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'unique5042')",
            settings=LAZY_APPLY_SETTINGS,
        ).strip()
        == "1"
    )

    # Merge across mixed-format parts: posting lists from the old pre-V1_WithCodec
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
    assert run_search_queries(node, table, settings=LAZY_APPLY_SETTINGS) == mixed_expected
    assert (
        node.query(
            f"SELECT count() FROM {table} WHERE hasToken(s, 'unique5042')",
            settings=LAZY_APPLY_SETTINGS,
        ).strip()
        == "1"
    )

    node.query(f"DROP TABLE {table} SYNC")
    node.restart_with_original_version()


# --------------------------------------------------------------------------------
# The tests below focus on the `text_index_serialization_version` MergeTree setting that controls
# the on-disk text index format, and on its interaction with the posting list codec
# across an upgrade and a downgrade.
# --------------------------------------------------------------------------------


# A part written by whatever binary is currently running: even k -> 'common',
# odd k -> 'rare', every row carries 'shared', and row 5042 carries the new token
# 'unique5042'. Mirrors the third part inserted by `test_text_index_upgrade`.
def insert_new_part(node, table):
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


# Expected `SEARCH_QUERIES` answers after `create_and_populate` followed by
# `insert_new_part`: 'common' and 'rare' each gain 1250 rows, 'unique42' stays on
# row 42, and the new token 'unique5042' lives once on row 5042.
MIXED_EXPECTED = [
    "3750",  # hasToken 'common'
    "3750",  # hasToken 'rare'
    "1",     # hasToken 'unique42'
    "0",     # hasToken 'absent'
    "3750",  # hasAllTokens ['common', 'shared']
    "3751",  # hasAnyTokens ['rare', 'unique42']
    "[42]",  # arraySort(groupArray(k)) for 'unique42'
]

NEW_TOKEN_QUERY = "SELECT count() FROM {table} WHERE hasToken(s, 'unique5042')"


def assert_single_active_part(node, table):
    active_parts = node.query(
        f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
    ).strip()
    assert active_parts == "1", (
        f"expected a single active part after OPTIMIZE FINAL, got {active_parts}"
    )


def assert_index_used(node, table):
    # A full scan would answer the search queries correctly too, so confirm the text
    # index is actually engaged via the query plan. The query must read a column: a
    # bare `count()` is answered from the index cardinality by `ReadFromTextIndexCount`,
    # whose plan has no `ReadFromMergeTree` step listing the used indexes.
    explain = node.query(
        f"EXPLAIN indexes = 1 SELECT k FROM {table} WHERE hasToken(s, 'unique42')"
    )
    assert "Name: idx" in explain, f"text index `idx` not used:\n{explain}"


def test_change_codec_after_upgrade(started_cluster):
    """Create the index on the old version with the default codec, upgrade, switch
    the posting list codec to 'bitpacking', and verify the index keeps working
    across parts written in two different on-disk codecs (and through a merge)."""
    node = started_cluster.instances["node"]
    table = "text_index_change_codec"

    # Old binary, default codec -> pre-V1_WithCodec ('v0_initial') header on disk.
    create_and_populate(node, table, posting_list_codec=None)
    assert run_search_queries(node, table) == expected_results()

    node.restart_with_latest_version()
    try:
        # New binary reads the old-format parts unchanged.
        assert run_search_queries(node, table) == expected_results()

        # Switch the default codec: new parts must persist the codec type in the header.
        node.query(
            f"ALTER TABLE {table} MODIFY SETTING text_index_posting_list_codec = 'bitpacking'"
        )

        # The version setting is only a preference: 'v0_initial' cannot persist the codec
        # type, so the write path silently bumps such an index to 'v1_with_codec'.
        node.query(
            f"ALTER TABLE {table} MODIFY SETTING text_index_serialization_version = 'v0_initial'"
        )

        # The new part is written with 'bitpacking' + the 'v1_with_codec' header, so the
        # table now mixes 'none'/'v0_initial' and 'bitpacking'/'v1_with_codec' segments.
        insert_new_part(node, table)
        assert run_search_queries(node, table) == MIXED_EXPECTED
        assert node.query(NEW_TOKEN_QUERY.format(table=table)).strip() == "1"
        assert_index_used(node, table)

        # Merge across the two codecs: the reader must decode both layouts and the
        # writer re-emits a single 'bitpacking'/'v1_with_codec' part.
        node.query(f"OPTIMIZE TABLE {table} FINAL")
        assert_single_active_part(node, table)
        assert run_search_queries(node, table) == MIXED_EXPECTED
        assert node.query(NEW_TOKEN_QUERY.format(table=table)).strip() == "1"

        node.query(f"DROP TABLE {table} SYNC")
    finally:
        node.restart_with_original_version()


def test_downgrade_after_writing_on_new_version(started_cluster):
    """The point of `text_index_serialization_version`: a new server can keep writing the old
    on-disk format so the data survives a rollback. Write 'v0_initial'-format parts
    with the *new* binary, reset the setting so the metadata stays loadable by the
    old binary, downgrade, and verify the old binary reads everything back."""
    node = started_cluster.instances["node"]
    table = "text_index_downgrade_setting"

    # Old binary, default codec -> 'v0_initial' format on disk.
    create_and_populate(node, table, posting_list_codec=None)
    assert run_search_queries(node, table) == expected_results()

    node.restart_with_latest_version()
    new_version_active = True
    try:
        # New binary reads the old-format parts unchanged.
        assert run_search_queries(node, table) == expected_results()

        # Force the new binary to keep writing the old on-disk format.
        node.query(
            f"ALTER TABLE {table} MODIFY SETTING text_index_serialization_version = 'v0_initial'"
        )

        # This part and the merged part below are written by the *new* binary, but in
        # the 'v0_initial' format because of the setting above.
        insert_new_part(node, table)
        assert run_search_queries(node, table) == MIXED_EXPECTED
        node.query(f"OPTIMIZE TABLE {table} FINAL")
        assert_single_active_part(node, table)
        assert run_search_queries(node, table) == MIXED_EXPECTED

        # An explicit `text_index_serialization_version` in the metadata is an unknown setting for
        # the old binary and would block ATTACH after the downgrade. Reset it; the
        # parts already on disk keep their 'v0_initial' format.
        node.query(f"ALTER TABLE {table} RESET SETTING text_index_serialization_version")

        node.restart_with_original_version()
        new_version_active = False

        # The old binary reads the parts the new binary wrote in 'v0_initial' format,
        # including the merged one. This is the downgrade guarantee. A 'v1_with_codec'
        # part here would instead fail to load on the old server.
        assert run_search_queries(node, table) == MIXED_EXPECTED
        assert node.query(NEW_TOKEN_QUERY.format(table=table)).strip() == "1"
        assert_index_used(node, table)

        node.query(f"DROP TABLE {table} SYNC")
    finally:
        if new_version_active:
            node.restart_with_original_version()


def test_downgrade_with_compatibility_setting(started_cluster):
    """The realistic rolling-upgrade knob: with `compatibility` pinned to a pre-26.6
    version in the default profile, the new server resolves `text_index_serialization_version` to
    'v0_initial' on its own, without persisting any setting into the table metadata, so
    the data stays readable after a rollback - no ALTER and no RESET required."""
    node = started_cluster.instances["node_compat"]
    table = "text_index_downgrade_compat"

    create_and_populate(node, table, posting_list_codec=None)
    assert run_search_queries(node, table) == expected_results()

    node.restart_with_latest_version()
    new_version_active = True
    try:
        assert run_search_queries(node, table) == expected_results()

        # No ALTER: `compatibility = '26.5'` from the default profile makes the new
        # binary write the 'v0_initial' format, and nothing is persisted in metadata.
        insert_new_part(node, table)
        assert run_search_queries(node, table) == MIXED_EXPECTED
        node.query(f"OPTIMIZE TABLE {table} FINAL")
        assert_single_active_part(node, table)
        assert run_search_queries(node, table) == MIXED_EXPECTED

        node.restart_with_original_version()
        new_version_active = False

        # The metadata never mentioned `text_index_serialization_version`, so the old binary loads
        # the table and reads the 'v0_initial'-format parts the new binary produced.
        assert run_search_queries(node, table) == MIXED_EXPECTED
        assert node.query(NEW_TOKEN_QUERY.format(table=table)).strip() == "1"
        assert_index_used(node, table)

        node.query(f"DROP TABLE {table} SYNC")
    finally:
        if new_version_active:
            node.restart_with_original_version()
