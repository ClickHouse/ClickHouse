"""E2E tests for Paimon over real S3 and Azure Blob Storage.

Runs in the "Integration tests E2E" CI job (pytest -m e2e), reusing the
data-lake e2e credentials (E2E_AWS_*, E2E_ICEBERG_AZURE_*). These cover the
real-cloud auth/endpoint/TLS paths the MinIO/Azurite suite
(test_storage_paimon_with_spark) cannot; the functional matrix (types,
pruning, schema evolution, Keeper state details) lives in that suite.
"""

import uuid

import pytest

from helpers.catalog_manager_paimon import (
    PaimonAzureManager,
    PaimonE2ECredentialsMissing,
    PaimonS3Manager,
)
from helpers.cluster import ClickHouseCluster
from helpers.paimon_utils import (
    TYPES_COLUMNS_SQL,
    TYPES_EXPECTED_DATA,
    TYPES_EXPECTED_SCHEMA,
    TYPES_NULLS_ROW,
    TYPES_VALUES_ROW,
    assert_cluster_fan_out,
    get_schema_and_data,
    id_array_str,
    parse_tsv,
    query_profile_events,
    wait_until_query_result,
)

pytestmark = pytest.mark.e2e

only_s3 = pytest.mark.only_backend("s3")
only_azure = pytest.mark.only_backend("azure")

_BACKENDS = {
    "azure": PaimonAzureManager,
    "s3": PaimonS3Manager,
}

_COMMON_MAIN_CONFIGS = ["configs/cluster.xml"]
_COMMON_USER_CONFIGS = ["configs/allow_experimental.xml"]


@pytest.fixture(scope="module", params=sorted(_BACKENDS))
def manager(request):
    try:
        mgr = _BACKENDS[request.param].from_env()
    except PaimonE2ECredentialsMissing as e:
        # Only absent credentials are skippable; any other from_env failure is
        # a setup regression and must fail, not silently drop the backend.
        pytest.skip(f"{request.param} e2e credentials not available: {e}")
    yield mgr
    mgr.cleanup_all()


@pytest.fixture(autouse=True)
def _skip_by_backend(request):
    marker = request.node.get_closest_marker("only_backend")
    if marker is not None:
        required = marker.args[0]
        # Read the parametrized backend WITHOUT building the manager, so an
        # @only_s3 test on an S3-only runner does not call
        # PaimonAzureManager.from_env (which fails on missing Azure creds).
        active = request.node.callspec.params.get("manager")
        if active != required:
            pytest.skip(f"requires {required} backend")


@pytest.fixture(scope="module")
def started_cluster(manager):
    cluster = ClickHouseCluster(__file__, name=f"test_e2e_paimon_{manager.backend}")
    cluster.add_instance(
        "node",
        main_configs=_COMMON_MAIN_CONFIGS,
        user_configs=_COMMON_USER_CONFIGS,
        with_zookeeper=True,
        stay_alive=True,
    )
    cluster.add_instance(
        "node2",
        main_configs=_COMMON_MAIN_CONFIGS,
        user_configs=_COMMON_USER_CONFIGS,
        with_zookeeper=True,
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def node(started_cluster):
    return started_cluster.instances["node"]


@pytest.fixture(scope="module")
def paimon_table(manager):
    """A shared append-only table with two snapshots: (1,'one'),(2,'two') + (3,'three')."""
    name = manager.create_table(
        "a INT NOT NULL, b STRING",
        ["VALUES (1, 'one'), (2, 'two')", "VALUES (3, 'three')"],
    )
    yield name
    manager.cleanup_table(name)


def test_table_function_select(node, manager, paimon_table):
    table_function = manager.table_function_sql(paimon_table)
    assert (
        node.query(f"SELECT a, b FROM {table_function} ORDER BY a")
        == "1\tone\n2\ttwo\n3\tthree\n"
    )
    assert node.query(f"SELECT count() FROM {table_function}") == "3\n"


@only_s3
def test_paimon_alias_table_function(node, manager, paimon_table):
    # `paimon` is an alias of `paimonS3`.
    table_function = manager.table_function_sql(paimon_table).replace(
        "paimonS3(", "paimon(", 1
    )
    assert node.query(f"SELECT count() FROM {table_function}") == "3\n"


def test_cluster_table_function(started_cluster, node, manager, paimon_table):
    expected = node.query(
        f"SELECT a, b FROM {manager.table_function_sql(paimon_table)} ORDER BY a"
    )
    cluster_table_function = manager.table_function_cluster_sql(
        "test_cluster", paimon_table
    )
    query_id = f"paimon-e2e-cluster-{uuid.uuid4().hex}"
    assert (
        node.query(
            f"SELECT a, b FROM {cluster_table_function} ORDER BY a", query_id=query_id
        )
        == expected
    )
    # Result equality alone cannot tell a real fan-out from a silent fallback
    # to the initiator-only path: the remote replica must have participated.
    assert_cluster_fan_out(started_cluster.instances["node2"], query_id)


def test_engine_select(node, manager, paimon_table):
    ch_table = f"paimon_eng_{uuid.uuid4().hex[:8]}"
    node.query(manager.create_engine_sql(ch_table, paimon_table))
    try:
        assert (
            node.query(f"SELECT a, b FROM {ch_table} ORDER BY a")
            == "1\tone\n2\ttwo\n3\tthree\n"
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {ch_table} SYNC")


def test_new_snapshot_visibility(node, manager):
    table_name = manager.create_table("a INT NOT NULL, b STRING", ["VALUES (1, 'one')"])
    try:
        table_function = manager.table_function_sql(table_name)
        assert node.query(f"SELECT count() FROM {table_function}") == "1\n"
        manager.append(table_name, "VALUES (2, 'two')")
        assert node.query(f"SELECT count() FROM {table_function}") == "2\n"
    finally:
        manager.cleanup_table(table_name)


def test_metadata_cache(node, manager, paimon_table):
    table_function = manager.table_function_sql(paimon_table)
    select_query = f"SELECT count() FROM {table_function} FORMAT Null"
    get_object_event = {"s3": "S3GetObject", "azure": "AzureGetObject"}[manager.backend]
    events = [
        "PaimonMetadataFilesCacheHits",
        "PaimonMetadataFilesCacheMisses",
        get_object_event,
    ]

    def profile_events(settings):
        return query_profile_events(node, select_query, events, settings=settings)

    node.query("SYSTEM DROP PAIMON METADATA CACHE")
    _, cold_misses, cold_gets = profile_events({"use_paimon_metadata_files_cache": 1})
    assert cold_misses > 0
    warm_hits, warm_misses, warm_gets = profile_events(
        {"use_paimon_metadata_files_cache": 1}
    )
    assert warm_misses == 0
    assert warm_hits > 0
    # The point of the cache over real cloud: strictly fewer GET requests.
    assert warm_gets < cold_gets


@only_azure
def test_azure_connection_string(node, manager, paimon_table):
    """The connection-string credential form of paimonAzure, against the real
    endpoint (parsing + auth path distinct from storage_account_url + key)."""
    table_function = manager.table_function_connection_string_sql(paimon_table)
    assert node.query(f"SELECT count() FROM {table_function}") == "3\n"


def test_all_types(node, manager):
    """The shared type matrix (identical to the emulator suite) over real cloud."""
    table_name = manager.create_table(
        TYPES_COLUMNS_SQL, [f"VALUES {TYPES_VALUES_ROW}", f"VALUES {TYPES_NULLS_ROW}"]
    )
    try:
        table_function = manager.table_function_sql(table_name)
        schema, data = get_schema_and_data(
            node,
            table_function,
            order_by="f_int_nn",
            settings={"session_timezone": "UTC"},
        )
        assert schema == TYPES_EXPECTED_SCHEMA
        assert data == TYPES_EXPECTED_DATA
    finally:
        manager.cleanup_table(table_name)


def test_partition_pruning(node, manager):
    """Pruning against real object storage, including a partition value that
    requires path escaping (real S3/Azure URL encoding differs from emulators)."""
    table_name = manager.create_table(
        "id BIGINT, part STRING",
        ["VALUES (1, 'plain'), (2, 'with space'), (3, 'x')"],
        partitioned_by="part",
    )
    try:
        table_function = manager.table_function_sql(table_name)
        for predicate, expected in (("part = 'plain'", "1\n"), ("part = 'with space'", "2\n")):
            files_read = {}
            for pruning in (0, 1):
                settings = {"use_paimon_partition_pruning": pruning}
                assert (
                    node.query(
                        f"SELECT id FROM {table_function} WHERE {predicate}",
                        settings=settings,
                    )
                    == expected
                )
                # The query condition cache would skip the files the query above found to have no match.
                files_read[pruning] = query_profile_events(
                    node,
                    f"SELECT id FROM {table_function} WHERE {predicate} FORMAT Null",
                    ["EngineFileLikeReadFiles"],
                    settings={**settings, "use_query_condition_cache": 0},
                )[0]
            # The pruning signal itself, not just result equality: a regression
            # that stops pruning would still return identical rows.
            assert files_read[1] < files_read[0], files_read
    finally:
        manager.cleanup_table(table_name)


def test_schema_evolution(node, manager):
    table_name = manager.create_table("a INT NOT NULL, b STRING", ["VALUES (1, 'one')"])
    try:
        spark_name = manager.spark_table(table_name)
        manager.run_spark_sql(table_name, f"ALTER TABLE {spark_name} ADD COLUMN c BIGINT")
        manager.append(table_name, "VALUES (2, 'two', 20)")
        manager.run_spark_sql(table_name, f"ALTER TABLE {spark_name} RENAME COLUMN b TO b2")
        manager.append(table_name, "VALUES (3, 'three', 30)")

        table_function = manager.table_function_sql(table_name)
        # Name-based resolution: files written before the rename read NULL for b2.
        assert (
            node.query(f"SELECT a, b2, c FROM {table_function} ORDER BY a")
            == "1\t\\N\t\\N\n2\t\\N\t20\n3\tthree\t30\n"
        )
    finally:
        manager.cleanup_table(table_name)


def test_compaction(node, manager):
    """DELETE manifest entries after compaction must be honored over real
    cloud — otherwise compacted-away files are silently double-counted."""
    table_name = manager.create_table(
        "id BIGINT, val STRING",
        [f"VALUES ({i}, 'x')" for i in range(3)],
        tblproperties={"compaction.min.file-num": "2"},
    )
    try:
        manager.compact(table_name)
        table_function = manager.table_function_sql(table_name)
        assert (
            node.query(f"SELECT count(), uniqExact(id) FROM {table_function}")
            == "3\t3\n"
        )
    finally:
        manager.cleanup_table(table_name)


def test_virtual_columns(node, manager, paimon_table):
    rows = parse_tsv(
        node.query(
            f"SELECT DISTINCT _file, _size > 0 FROM {manager.table_function_sql(paimon_table)}"
        )
    )
    assert len(rows) == 2  # two inserts -> two data files
    for file, size_positive in rows:
        assert file.endswith(".parquet")
        assert size_positive == "1"


def test_unsupported_row_type(node, manager):
    table_name = manager.create_table(
        "id INT, nested STRUCT<x: INT>",
        ["VALUES (1, named_struct('x', 1))"],
    )
    try:
        error = node.query_and_get_error(
            f"DESC {manager.table_function_sql(table_name)}"
        )
        assert "Unsupported Paimon type: ROW" in error
    finally:
        manager.cleanup_table(table_name)


def test_nonexistent_table_path(node, manager):
    table_function = manager.table_function_sql(f"no_such_table_{uuid.uuid4().hex[:8]}")
    error = node.query_and_get_error(f"SELECT count() FROM {table_function}")
    assert "DB::Exception" in error
    assert node.query("SELECT 1") == "1\n"


def test_incremental_read(node, manager):
    table_name = manager.create_table("id BIGINT, val STRING", ["VALUES (0, 'warmup')"])
    ch_table = f"paimon_inc_{uuid.uuid4().hex[:8]}"
    try:
        node.query(
            manager.create_engine_sql(ch_table, table_name)
            + f" SETTINGS paimon_incremental_read = 1,"
            f" paimon_keeper_path = '/clickhouse/paimon/{ch_table}',"
            f" paimon_replica_name = 'r1',"
            f" paimon_metadata_refresh_interval_sec = 1"
        )
        count_query = f"SELECT count() FROM {ch_table}"
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {ch_table}"
        # First read is a full base scan establishing the watermark; then drained.
        wait_until_query_result(node, count_query, "1\n")
        wait_until_query_result(node, count_query, "0\n")

        # Two pending snapshots with distinct id sets: snapshot 2 (1..10) and
        # snapshot 3 (11..20). Distinct sets are what makes the targeted read
        # provable: an implementation returning "the current pending delta"
        # (1..20) or the wrong snapshot fails the id assertions below.
        manager.append(table_name, "SELECT id + 1, 'x' FROM range(10)")
        manager.append(table_name, "SELECT id + 11, 'x' FROM range(10)")

        # Targeted snapshot reads BEFORE any regular drain: each delivers
        # exactly the requested snapshot's ids, repeatably, and must not
        # advance the watermark.
        target_2 = f"{ids_query} SETTINGS paimon_target_snapshot_id = 2"
        wait_until_query_result(node, target_2, id_array_str(range(1, 11)))
        wait_until_query_result(node, target_2, id_array_str(range(1, 11)))
        target_3 = f"{ids_query} SETTINGS paimon_target_snapshot_id = 3"
        wait_until_query_result(node, target_3, id_array_str(range(11, 21)))

        # The regular read must still deliver both snapshots in full — the id
        # set proves the targeted reads did not advance the watermark.
        wait_until_query_result(node, ids_query, id_array_str(range(1, 21)))
        wait_until_query_result(node, count_query, "0\n")

        # Bounded draining: two pending snapshots consumed one at a time, each
        # delivering exactly its own id set (no replay, no drop).
        manager.append(table_name, "SELECT id + 21, 'x' FROM range(10)")
        manager.append(table_name, "SELECT id + 31, 'x' FROM range(10)")
        limited_query = f"{ids_query} SETTINGS max_consume_snapshots = 1"
        wait_until_query_result(node, limited_query, id_array_str(range(21, 31)))
        wait_until_query_result(node, limited_query, id_array_str(range(31, 41)))
        wait_until_query_result(node, limited_query, "[]\n")
    finally:
        node.query(f"DROP TABLE IF EXISTS {ch_table} SYNC")
        manager.cleanup_table(table_name)
