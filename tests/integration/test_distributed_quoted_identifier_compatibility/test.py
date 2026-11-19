import pytest

from helpers.cluster import ClickHouseCluster

# Predates #101048, so it spells a plan column identifier without quotes.
LEGACY_VERSION = "26.3"

cluster = ClickHouseCluster(__file__)
current = cluster.add_instance(
    "current",
    main_configs=["configs/remote_servers.xml"],
    # Not covered: the SQL a serialized plan falls back to for an older replica, whose header is
    # planned without the context that holds the old spellings.
    use_distributed_plan=False,
)
legacy = cluster.add_instance(
    "legacy",
    main_configs=["configs/remote_servers.xml"],
    image="clickhouse/clickhouse-server",
    tag=LEGACY_VERSION,
    with_installed_binary=True,
)

# One column per spelling that writeProbablyQuotedStringImpl quotes: a dot, six of the words it
# quotes as keywords, a non-word character and a leading digit. `group`/`order`/`where`/`join`/
# `union` are NOT quoted and serve as controls.
COLUMNS = [
    "a.b",
    "null",
    "distinct",
    "all",
    "select",
    "from",
    "values",
    "a-b",
    "1abc",
    "group",
    "order",
    "where",
    "join",
    "union",
]
UNQUOTED_COLUMNS = ["plain", "_ok", "ab1"]
ALL_COLUMNS = COLUMNS + UNQUOTED_COLUMNS


def _create_table(node):
    columns = ", ".join(f"`{name}` UInt64" for name in ALL_COLUMNS)
    node.query("DROP TABLE IF EXISTS quoted_ids SYNC")
    node.query(
        f"CREATE TABLE quoted_ids ({columns}, arr Array(UInt64), n Nested(k UInt8), v UInt64) "
        f"ENGINE = MergeTree ORDER BY v"
    )
    values = ", ".join(f"number + {i}" for i in range(len(ALL_COLUMNS)))
    node.query(
        f"INSERT INTO quoted_ids SELECT {values}, [number], [number % 2], number FROM numbers(5)"
    )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        for node in (current, legacy):
            _create_table(node)
        yield cluster
    finally:
        cluster.shutdown()


def _remote_of(initiator, table):
    # Both shards are named explicitly so the query always crosses the version boundary, whichever
    # node is the initiator.
    other = "legacy" if initiator is current else "current"
    return f"remote('{initiator.name},{other}', currentDatabase(), {table})"


def _remote(initiator):
    return _remote_of(initiator, "quoted_ids")


# 5 rows per shard, 2 shards, values start at `number + index of the column`.
def _expected_sum(column):
    return 2 * sum(n + ALL_COLUMNS.index(column) for n in range(5))


def _same_version_answer(query):
    # Both nodes hold identical data, so the answer without crossing the boundary is the expected one.
    return current.query(
        query.format(remote="remote('current,current', currentDatabase(), quoted_ids)")
        + " SETTINGS prefer_localhost_replica = 0"
    )


@pytest.mark.parametrize("column", ALL_COLUMNS)
@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_quoted_identifier_over_version_boundary(start_cluster, initiator, column):
    # An intermediate stage is what puts plan column identifiers on the wire, so the aggregate must
    # not be merged on the shard.
    assert initiator.query(
        f"SELECT sum(`{column}`) FROM {_remote(initiator)}"
    ).strip() == str(_expected_sum(column))


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_composite_action_name(start_cluster, initiator):
    # The transported header carries the identifier embedded in a function's action name.
    # Two shards of rows 0..4: v odd gives (1+1)+(3+1) per shard, v even (0+1)+(2+1)+(4+1).
    assert initiator.query(
        f"SELECT sum(plus(`a.b`, 1)) FROM {_remote(initiator)} GROUP BY v % 2 ORDER BY 1"
    ).split() == ["12", "18"]


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_union(start_cluster, initiator):
    # Proves the rename happens once on the reconciled header and that a branch's collected
    # provenance outlived its branch planner.
    remote = _remote(initiator)
    assert initiator.query(
        f"SELECT sum(s) FROM (SELECT sum(`a.b`) s FROM {remote}"
        f" UNION ALL SELECT sum(`select`) s FROM {remote})"
    ).strip() == str(_expected_sum("a.b") + _expected_sum("select"))


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_grouping_sets(start_cluster, initiator):
    # The header carries a renameable quoted identifier next to the planner-generated
    # `__grouping_set`, which has no query tree expression and needs no rename.
    assert initiator.query(
        f"SELECT sum(x) FROM (SELECT sum(`a.b`) x FROM {_remote(initiator)}"
        f" GROUP BY GROUPING SETS ((`a.b`), (v)))"
    ).strip() == str(2 * _expected_sum("a.b"))


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_lambda_argument(start_cluster, initiator):
    # A lambda argument name is emitted verbatim, so it can look exactly like a qualifier.
    assert initiator.query(
        f"SELECT sum(`a.b`), sum(arrayMap(x -> x + 1, arr)[1]) FROM {_remote(initiator)}"
    ).split() == [str(_expected_sum("a.b")), "30"]


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_order_by_quoted_identifier(start_cluster, initiator):
    assert (
        initiator.query(
            f"SELECT `a.b` FROM {_remote(initiator)} ORDER BY `a.b` LIMIT 1"
        ).strip()
        == "0"
    )


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_column_selected_twice(start_cluster, initiator):
    # Two header entries with the same name read one received column. Sorted here rather than with
    # `ORDER BY ALL`, which the column named `all` makes ambiguous.
    for query in (
        "SELECT * FROM (SELECT `a.b`, `a.b` FROM {remote} LIMIT 100)",
        "SELECT * FROM (SELECT *, `a.b` FROM {remote} LIMIT 100)",
    ):
        assert sorted(
            initiator.query(query.format(remote=_remote(initiator))).splitlines()
        ) == sorted(_same_version_answer(query).splitlines())


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_nested_subcolumn_in_aggregate(start_cluster, initiator):
    # A `Nested` subcolumn inside an aggregate's argument, the shape reported from a production roll.
    query = (
        "SELECT v % 2 AS g, argMax(has(n.k, 0), v) FROM {remote} GROUP BY g ORDER BY g"
    )
    assert initiator.query(
        query.format(remote=_remote(initiator))
    ) == _same_version_answer(query)


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_distinct_spellings_of_one_name(start_cluster, initiator):
    # A column named `table` and a column named literally `` `table` ``: each convention spells the
    # two distinctly, so both values must come back, and to the right column.
    other = "legacy" if initiator is current else "current"
    for node in (current, legacy):
        node.query("DROP TABLE IF EXISTS two_spellings SYNC")
        node.query(
            "CREATE TABLE two_spellings (`table` UInt64, `\\`table\\`` UInt64, v UInt64) "
            "ENGINE = MergeTree ORDER BY v"
        )
        node.query(
            "INSERT INTO two_spellings SELECT number, number + 100, number FROM numbers(3)"
        )

    assert initiator.query(
        "SELECT sum(`table`), sum(`\\`table\\``) FROM "
        f"remote('{initiator.name},{other}', currentDatabase(), two_spellings)"
    ).split() == ["6", "606"]


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_user_text_is_not_rewritten(start_cluster, initiator):
    assert initiator.query(
        f"SELECT sum(`a.b`), any('__table1.') FROM {_remote(initiator)}"
    ).split() == [str(_expected_sum("a.b")), "__table1."]


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_stage_after_aggregation(start_cluster, initiator):
    assert (
        initiator.query(
            f"SELECT sum(`a.b`) FROM {_remote(initiator)}"
            f" SETTINGS distributed_group_by_no_merge = 2"
        ).split()
        == [str(_expected_sum("a.b") // 2)] * 2
    )


def test_same_version_is_unaffected(start_cluster):
    # Every case above, run without crossing the boundary. A same-version peer must keep taking the
    # existing exact-name path. `prefer_localhost_replica = 0` is what makes this a real TCP hop:
    # with the default the initiator serves both shards as local plans, so no packet is exchanged
    # and nothing about the transported header would be exercised.
    for column in ALL_COLUMNS:
        remote_answer = current.query(
            f"SELECT sum(`{column}`) FROM remote('current,current', currentDatabase(), quoted_ids)"
            f" SETTINGS prefer_localhost_replica = 0"
        ).strip()
        # Compared against the local answer for the same column, so a swap of two same-version
        # columns fails here rather than agreeing with a wrong expectation.
        local_answer = current.query(
            f"SELECT 2 * sum(`{column}`) FROM quoted_ids"
        ).strip()
        assert remote_answer == local_answer == str(_expected_sum(column))


@pytest.mark.parametrize(
    "initiator", [current, legacy], ids=["new_reads_old", "old_reads_new"]
)
def test_colliding_legacy_spelling_is_refused(start_cluster, initiator):
    # `__actionName`'s second argument becomes the action node name verbatim, so a header can carry
    # a name equal to another entry's legacy spelling. Both ends must refuse such a header rather
    # than resolve one column for two.
    #
    # The expected codes differ by direction: the sender cannot produce an unambiguous header
    # (AMBIGUOUS_COLUMN_NAME), while a receiver cannot map the received block onto its own columns
    # (NOT_FOUND_COLUMN_IN_BLOCK). The messages are asserted too, because unpatched servers also
    # fail this query with NOT_FOUND_COLUMN_IN_BLOCK, for the ordinary missing-column reason.
    expected = (
        "has no unambiguous pre-"
        if initiator is legacy
        else "do not map one to one onto the expected columns"
    )
    error = initiator.query_and_get_error(
        "SELECT DISTINCT `a.b`, __actionName('x', '__table1.a.b') "
        f"FROM {_remote(initiator)} ORDER BY 1"
    )
    assert expected in error, error


def test_file_cluster_over_version_boundary(start_cluster):
    # `IStorageCluster` computes a current-format header and then receives intermediate blocks from
    # its workers, so it breaks the same way as `remote()`. One case exercises the shared
    # read/ReadFromCluster seam that serves StorageFileCluster, StorageURLCluster and
    # StorageObjectStorageCluster; it does not prove each subclass separately.
    #
    # The worker set is a single legacy node, so the one read task cannot be served without
    # crossing the boundary. Over `mixed_versions` the current worker may take the task alone and
    # the case would then pass with the legacy path never entered.
    #
    # The initiator resolves the path and sends it, so the file is written on both nodes even
    # though only the legacy worker reads it.
    for node in (current, legacy):
        node.query(
            "INSERT INTO TABLE FUNCTION "
            "file('quoted.csv', 'CSVWithNames', '`a.b` UInt64, v UInt64') "
            "SETTINGS engine_file_truncate_on_insert = 1 "
            "VALUES (1, 10), (2, 20), (3, 30)"
        )

    assert (
        current.query(
            "SELECT sum(`a.b`) FROM fileCluster("
            "'legacy_only', 'quoted.csv', 'CSVWithNames', '`a.b` UInt64, v UInt64')"
        ).strip()
        == "6"
    )
