"""
Delta Lake writes against tables with writer features (Spark-created): every INSERT is either
rejected before anything is committed or commits data Spark reads back consistently. Plus a
failpoint matrix over the stages of a write on S3, and S3 faults injected by the broken_s3 mock
(in proxy mode, in front of MinIO) at the data-file and the commit upload.
"""

import json
import logging
import os
import threading

import pyarrow as pa
import pyspark
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers
from helpers.s3_mocks.broken_s3 import MockControl
from helpers.s3_tools import LocalDownloader, LocalUploader
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config
from test_storage_delta.test import (
    create_empty_delta_table,
    list_delta_data_files,
    randomize_table_name,
)

USER_FILES = "/var/lib/clickhouse/user_files"
MOCK_PORT = "8083"

cluster = ClickHouseCluster(__file__, with_spark=True)


def get_spark(started_cluster):
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_writes_table_features")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.warehouse", USER_FILES)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.clusteredTable.enableClusteringTablePreview", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .master("local")
    )
    props_path = write_spark_log_config(started_cluster.instances_dir)
    builder = builder.config("spark.driver.extraJavaOptions", f"-Dlog4j2.configurationFile=file:{props_path}")
    return builder.getOrCreate()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            # No S3 retries on ClickHouse's own client: persistent faults fail within the test budget.
            main_configs=["configs/config.d/disable_s3_retries.xml"],
            user_configs=["configs/users.d/users.xml", "configs/users.d/enable_writes.xml"],
            with_minio=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        if int(cluster.instances["node1"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip("DeltaLake engine is not available")
        # broken_s3 as a forwarding proxy in front of MinIO (a redirecting mock would make the
        # kernel's SigV4 signature invalid at the upstream).
        start_mock_servers(
            cluster,
            os.path.join(os.path.dirname(start_mock_servers.__code__.co_filename), "s3_mocks"),
            [("broken_s3.py", "resolver", MOCK_PORT, ["minio1", "9001", "proxy"])],
        )
        cluster.broken_s3 = MockControl(cluster, "resolver", MOCK_PORT)
        cluster.spark_session = ResilientSparkSession(lambda: get_spark(cluster))
        yield cluster
    finally:
        cluster.shutdown()


def push_to_node(node, path):
    LocalUploader(node).upload_directory(f"{path}/", f"{path}/")


def pull_from_node(node, path):
    LocalDownloader(node).download_directory(f"{path}/", f"{path}/")


# ---------------------------------------------------------------------------------------------
# writer features
# ---------------------------------------------------------------------------------------------

# name -> (DDL after "CREATE TABLE delta.`path` (id INT, v STRING) USING delta", extra SQL, a row
# that violates the feature's constraint or None)
FEATURES = {
    "append_only": ("TBLPROPERTIES (delta.appendOnly = true)", None, None),
    "not_null": (None, None, "(NULL, 'x')"),
    "check_constraint": (None, "ALTER TABLE {t} ADD CONSTRAINT positive CHECK (id > 0)", "(-1, 'x')"),
    "generated_column": (None, None, "(3, 'bad', 999)"),
    "change_data_feed": ("TBLPROPERTIES (delta.enableChangeDataFeed = true)", None, None),
    "deletion_vectors": ("TBLPROPERTIES (delta.enableDeletionVectors = true)", None, None),
    "liquid_clustering": ("CLUSTER BY (id)", None, None),
    "row_tracking": ("TBLPROPERTIES (delta.enableRowTracking = true)", None, None),
    "timestamp_ntz": (None, None, None),
    "column_mapping_name": ("TBLPROPERTIES (delta.columnMapping.mode = 'name')", None, None),
    "identity_column": (None, None, None),
    "in_commit_timestamps": ("TBLPROPERTIES (delta.enableInCommitTimestamps = true)", None, None),
    "type_widening": ("TBLPROPERTIES (delta.enableTypeWidening = true)", "ALTER TABLE {t} ALTER COLUMN id TYPE BIGINT", None),
    "variant_column": (None, None, None),
}


# Outcome observed with the pinned kernel: a change in either direction must be a deliberate edit here.
# Features missing from the map are kernel-decided in a way not pinned yet (Spark in the runner could
# not create them when this was written).
EXPECTED_OUTCOME = {
    "append_only": "accepted",
    "not_null": "accepted",
    "deletion_vectors": "accepted",
    "timestamp_ntz": "accepted",
    "change_data_feed": "rejected",
    "check_constraint": "rejected",
    "column_mapping_name": "rejected",
    "liquid_clustering": "rejected",
}


def create_feature_table(spark, name, path):
    table = f"delta.`{path}`"
    columns = "id INT, v STRING"
    if name == "not_null":
        columns = "id INT NOT NULL, v STRING"
    elif name == "generated_column":
        columns = "id INT, v STRING, id2 INT GENERATED ALWAYS AS (id + 1)"
    elif name == "timestamp_ntz":
        columns = "id INT, v STRING, ts TIMESTAMP_NTZ"
    elif name == "identity_column":
        columns = "id INT, v STRING, seq BIGINT GENERATED ALWAYS AS IDENTITY"
    elif name == "variant_column":
        columns = "id INT, v STRING, var VARIANT"
    clause, extra, _ = FEATURES[name]
    spark.sql(f"CREATE TABLE {table} ({columns}) USING delta {clause or ''}")
    if extra:
        spark.sql(extra.format(t=table))
    if name == "identity_column":
        spark.sql(f"INSERT INTO {table} (id, v) VALUES (1, 'spark')")
    elif name == "variant_column":
        spark.sql(f"INSERT INTO {table} VALUES (1, 'spark', parse_json('{{\"k\": 1}}'))")
    else:
        spark.sql(f"INSERT INTO {table} VALUES (1, 'spark'" + (", TIMESTAMP_NTZ '2024-01-01 00:00:00'" if name == "timestamp_ntz" else "") + ")")


@pytest.mark.parametrize(
    "feature",
    [
        pytest.param(
            f,
            marks=pytest.mark.xfail(
                strict=True,
                reason="https://github.com/ClickHouse/ClickHouse/issues/120653: the written timestamp is UTC-adjusted, Spark cannot read a timestamp_ntz column",
            ),
        )
        if f == "timestamp_ntz"
        else f
        for f in sorted(FEATURES)
    ],
)
def test_write_to_table_with_writer_feature(started_cluster, feature):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name(f"test_feature_{feature}")
    path = f"{USER_FILES}/{table_name}"
    table = f"delta.`{path}`"
    try:
        create_feature_table(spark, feature, path)
    except Exception as e:  # pylint: disable=broad-except
        pytest.skip(f"Spark cannot create a table with {feature} here: {str(e)[:200]}")
    push_to_node(node, path)

    ch_columns = "id Int32, v String"
    if feature == "not_null":
        ch_columns = "id Nullable(Int32), v String"
    elif feature == "generated_column":
        ch_columns = "id Int32, v String, id2 Int32"
    elif feature == "timestamp_ntz":
        ch_columns = "id Int32, v String, ts DateTime64(6)"
    elif feature == "identity_column":
        ch_columns = "id Int32, v String, seq Int64"
    elif feature == "type_widening":
        ch_columns = "id Int64, v String"
    elif feature == "variant_column":
        ch_columns = "id Int32, v String, var String"
    _, error = node.query_and_get_answer_with_error(f"CREATE TABLE {table_name} ({ch_columns}) ENGINE = DeltaLakeLocal('{path}')")
    if error:
        # The table cannot even be attached (e.g. a Variant column): nothing to write into, nothing committed.
        logging.info("%s: attach rejected: %s", feature, error.splitlines()[0][:200])
        assert "NOT_IMPLEMENTED" in error or "DELTA_KERNEL_ERROR" in error, error
        assert spark.sql(f"DESCRIBE HISTORY {table}").count() == 2
        return

    versions_before = spark.sql(f"DESCRIBE HISTORY {table}").count()
    # type_widening: a value that only fits the widened BIGINT column.
    new_id = 3000000000 if feature == "type_widening" else 2
    valid_row = {
        "generated_column": "(2, 'clickhouse', 3)",
        "timestamp_ntz": "(2, 'clickhouse', '2024-06-01 12:00:00')",
        "identity_column": "(2, 'clickhouse', 2)",
        "variant_column": "(2, 'clickhouse', '{\"k\": 2}')",
    }.get(feature, f"({new_id}, 'clickhouse')")
    _, error = node.query_and_get_answer_with_error(f"INSERT INTO {table_name} VALUES {valid_row}")
    pull_from_node(node, path)
    versions_after = spark.sql(f"DESCRIBE HISTORY {table}").count()
    rows = spark.sql(f"SELECT id, v FROM {table} ORDER BY id").collect()

    if feature in EXPECTED_OUTCOME:
        assert ("rejected" if error else "accepted") == EXPECTED_OUTCOME[feature], (feature, error)
    if error:
        # Fail closed: a kernel/engine rejection with nothing committed.
        logging.info("%s: rejected: %s", feature, error.splitlines()[0][:200])
        assert "DELTA_KERNEL_ERROR" in error or "NOT_IMPLEMENTED" in error, error
        assert versions_after == versions_before
        assert [(r.id, r.v) for r in rows] == [(1, "spark")]
        return

    logging.info("%s: accepted", feature)
    assert versions_after == versions_before + 1
    assert [(r.id, r.v) for r in rows] == [(1, "spark"), (new_id, "clickhouse")], rows
    # The feature column itself must read back in Spark, not only `id` and `v`.
    if feature == "generated_column":
        assert spark.sql(f"SELECT id2 FROM {table} WHERE id = 2").collect()[0].id2 == 3
    if feature == "identity_column":
        assert spark.sql(f"SELECT seq FROM {table} WHERE id = 2").collect()[0].seq == 2
    if feature == "variant_column":
        assert spark.sql(f"SELECT to_json(var) AS j FROM {table} WHERE id = 2").collect()[0].j == '{"k":2}'
    if feature == "timestamp_ntz":
        assert str(spark.sql(f"SELECT ts FROM {table} WHERE id = 2").collect()[0].ts) == "2024-06-01 12:00:00"
        assert node.query(f"SELECT ts FROM {table_name} WHERE id = 2").strip() == "2024-06-01 12:00:00.000000"
    assert node.query(f"SELECT count() FROM {table_name}").strip() == "2"
    if feature == "identity_column":
        assert node.query(f"SELECT seq FROM {table_name} WHERE id = 2").strip() == "2"
    if feature == "variant_column":
        assert node.query(f"SELECT var FROM {table_name} WHERE id = 2").strip() == '{"k":2}'

    violating = FEATURES[feature][2]
    if violating:
        # The write was accepted, so the feature's constraint must be enforced.
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {table_name} VALUES {violating}")
        assert error, f"{feature}: a row violating the constraint was committed"
        pull_from_node(node, path)
        assert spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0].c == 2


def test_write_to_table_with_unknown_writer_feature_is_rejected(started_cluster):
    """Protocol (3, 7) with a writer feature this kernel does not know: the INSERT must be refused
    before anything is committed, whatever the feature turns out to mean."""
    node = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_unknown_writer_feature")
    path = f"{USER_FILES}/{table_name}"
    os.makedirs(f"{path}/_delta_log", exist_ok=True)
    schema = '{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"v","type":"string","nullable":true,"metadata":{}}]}'
    with open(f"{path}/_delta_log/00000000000000000000.json", "w") as f:
        f.write('{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["futureWriterFeatureFromANewerProtocol"]}}\n')
        f.write(
            '{"metaData":{"id":"' + table_name + '","format":{"provider":"parquet","options":{}},"schemaString":"'
            + schema.replace('"', '\\"')
            + '","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}\n'
        )
    push_to_node(node, path)

    _, error = node.query_and_get_answer_with_error(f"CREATE TABLE {table_name} (id Int32, v String) ENGINE = DeltaLakeLocal('{path}')")
    if not error:
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {table_name} VALUES (1, 'clickhouse')")
    assert error, "an INSERT into a table with an unknown writer feature was committed"
    assert "DELTA_KERNEL_ERROR" in error or "NOT_IMPLEMENTED" in error, error
    pull_from_node(node, path)
    assert sorted(os.listdir(f"{path}/_delta_log")) == ["00000000000000000000.json"]
    assert [f for f in os.listdir(path) if f.endswith(".parquet")] == []


def log_versions(started_cluster, path):
    return sorted(
        int(obj.object_name.rsplit("/", 1)[1][:-5])
        for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/_delta_log/", recursive=True)
        if obj.object_name.endswith(".json")
    )


# ---------------------------------------------------------------------------------------------
# failpoint matrix: fail at every stage of the write, on every storage type
# ---------------------------------------------------------------------------------------------

FAILPOINT_STAGES = {
    # consume(): a row throws while data files are open
    "throw_in_consume": (None, "SELECT (throwIf(number = 5, 'boom') + number)::Int32", "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
    # onFinish(): data files finalized, commit not started
    "throw_before_commit": ("delta_lake_write_throw_before_commit", "SELECT number::Int32", "FAULT_INJECTED"),
    # onFinish(): commit done, cancel arrives before the sinks are released
    "cancel_in_commit_window": ("delta_lake_write_cancel_in_commit_window", "SELECT number::Int32", "QUERY_WAS_CANCELLED"),
}


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("stage", sorted(FAILPOINT_STAGES))
def test_failpoint_matrix_on_s3(started_cluster, stage, partitioned):
    node = started_cluster.instances["node1"]
    failpoint, select, expected_error = FAILPOINT_STAGES[stage]
    path = randomize_table_name(f"test_fp_{stage}")
    schema = pa.schema([("id", pa.int32(), False), ("part", pa.int32(), False)])
    create_empty_delta_table(started_cluster, "s3", path, schema, partition_by=["part"] if partitioned else None)
    engine = f"DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{path}/', 'minio', '{minio_secret_key}')"
    node.query(f"CREATE TABLE {path} (id Int32, part Int32) ENGINE = {engine}")
    if failpoint:
        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    try:
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {path} {select} AS id, (number % 2)::Int32 AS part FROM numbers(10) SETTINGS max_block_size = 1")
    finally:
        if failpoint:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
    assert expected_error in error, error
    assert node.query("SELECT 1").strip() == "1"

    versions = log_versions(started_cluster, path)
    data_files = list_delta_data_files(started_cluster, "s3", path)
    if stage == "cancel_in_commit_window":
        # Committed before the cancel: data must survive and stay referenced.
        assert versions == [0, 1], versions
        assert len(data_files) == (2 if partitioned else 1), data_files
        assert node.query(f"SELECT count() FROM {path}").strip() == "10"
    else:
        # Failed before the commit: nothing committed, no orphan.
        assert versions == [0], versions
        assert data_files == [], data_files
        assert node.query(f"SELECT count() FROM {path}").strip() == "0"
    node.query(f"DROP TABLE {path}")


# ---------------------------------------------------------------------------------------------
# S3 faults at the data-file upload and at the commit upload
# ---------------------------------------------------------------------------------------------


def mock_engine_definition(started_cluster, path):
    return f"DeltaLake('http://resolver:{MOCK_PORT}/{started_cluster.minio_bucket}/{path}/', 'minio', '{minio_secret_key}')"


def _new_mock_table(started_cluster, node, name, partitioned):
    path = randomize_table_name(name)
    schema = pa.schema([("id", pa.int32(), False), ("part", pa.int32(), False)])
    create_empty_delta_table(started_cluster, "s3", path, schema, partition_by=["part"] if partitioned else None)
    node.query(f"CREATE TABLE {path} (id Int32, part Int32) ENGINE = {mock_engine_definition(started_cluster, path)}")
    node.query(f"INSERT INTO {path} SELECT number AS id, number % 2 AS part FROM numbers(6)")
    _assert_consistent(started_cluster, node, path, 1, partitioned)
    return path


def _insert_through_fault(started_cluster, node, path, stage, action, count, partitioned):
    broken_s3 = started_cluster.broken_s3
    broken_s3.reset()
    # The data files are the first single-object PUTs of an INSERT (one per partition), the commit
    # JSON the one after them.
    data_files_per_insert = 2 if partitioned else 1
    broken_s3.setup_at_object_upload(count=count, after=0 if stage == "data_file" else data_files_per_insert, action=action)
    try:
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {path} SELECT number + 100 AS id, number % 2 AS part FROM numbers(6)")
    finally:
        broken_s3.reset()
    logging.info("%s/%s/%s: %s", stage, action, count, (error or "no error").splitlines()[0][:200])
    return error


def _assert_consistent(started_cluster, node, path, committed_inserts, partitioned):
    assert log_versions(started_cluster, path) == list(range(committed_inserts + 1))
    files_per_insert = 2 if partitioned else 1
    assert len(list_delta_data_files(started_cluster, "s3", path)) == committed_inserts * files_per_insert
    assert node.query(f"SELECT count() FROM {path}").strip() == str(committed_inserts * 6)


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("stage", ["data_file", "commit"])
@pytest.mark.parametrize("action", ["connection_reset_by_peer", "slow_down"])
def test_transient_s3_fault(started_cluster, stage, action, partitioned):
    node = started_cluster.instances["node1"]
    path = _new_mock_table(started_cluster, node, f"test_transient_{stage}_{action}", partitioned)
    error = _insert_through_fault(started_cluster, node, path, stage, action, 1, partitioned)
    # Retried past the fault, or failed closed: never a half-committed table.
    _assert_consistent(started_cluster, node, path, 1 if error else 2, partitioned)
    node.query(f"DROP TABLE {path}")


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("stage", ["data_file", "commit"])
def test_persistent_s3_fault_fails_closed(started_cluster, stage, partitioned):
    node = started_cluster.instances["node1"]
    path = _new_mock_table(started_cluster, node, f"test_persistent_{stage}", partitioned)
    error = _insert_through_fault(started_cluster, node, path, stage, "connection_reset_by_peer", 100000, partitioned)
    assert error, "the INSERT succeeded although every upload was reset"
    _assert_consistent(started_cluster, node, path, 1, partitioned)
    # The table keeps working once the fault is gone.
    node.query(f"INSERT INTO {path} SELECT number + 200 AS id, number % 2 AS part FROM numbers(6)")
    _assert_consistent(started_cluster, node, path, 2, partitioned)
    node.query(f"DROP TABLE {path}")


def _all_parquet_objects(started_cluster, path):
    return {obj.object_name for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/", recursive=True) if obj.object_name.endswith(".parquet")}


def _committed_add_objects(started_cluster, path):
    result = set()
    for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/_delta_log/", recursive=True):
        if not obj.object_name.endswith(".json"):
            continue
        for line in started_cluster.minio_client.get_object(started_cluster.minio_bucket, obj.object_name).read().decode().splitlines():
            action = json.loads(line)
            if "add" in action:
                result.add(f"{path}/{action['add']['path']}")
    return result


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/ClickHouse/ClickHouse/issues/112096: a lost response on the commit PUT makes the writer delete the data files of the version it just committed",
)
def test_lost_commit_response_keeps_committed_data(started_cluster, partitioned):
    """The commit PUT reaches the object storage and lands as version 1, but the writer never sees
    the 200. Whatever the INSERT then reports, the log must never reference a missing data file and
    the table must stay readable."""
    node = started_cluster.instances["node1"]
    path = _new_mock_table(started_cluster, node, "test_lost_commit_response", partitioned)
    error = _insert_through_fault(started_cluster, node, path, "commit", "lost_response", 1, partitioned)
    logging.info("lost commit response: %s", (error or "acknowledged").splitlines()[0][:200])
    # The PUT was applied upstream: version 2 exists.
    assert log_versions(started_cluster, path) == [0, 1, 2]
    missing = _committed_add_objects(started_cluster, path) - _all_parquet_objects(started_cluster, path)
    assert not missing, f"committed data files were deleted: {missing}"
    assert node.query(f"SELECT count() FROM {path}").strip() == "12"
    node.query(f"DROP TABLE {path}")


@pytest.mark.parametrize("partitioned", [False, True])
def test_commit_failure_with_failing_cleanup(started_cluster, partitioned):
    """The commit fails and the removal of the already-uploaded data files fails too: the INSERT
    reports an error, the table is unchanged and readable, the next INSERT works. The uploaded
    files may remain as orphans (documented; nothing references them)."""
    node = started_cluster.instances["node1"]
    broken_s3 = started_cluster.broken_s3
    path = _new_mock_table(started_cluster, node, "test_commit_and_cleanup_fail", partitioned)
    data_files_per_insert = 2 if partitioned else 1
    broken_s3.reset()
    broken_s3.setup_at_object_upload(count=100000, after=data_files_per_insert, action="connection_reset_by_peer")
    broken_s3.setup_at_object_delete(count=100000, action="connection_reset_by_peer")
    try:
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {path} SELECT number + 100 AS id, number % 2 AS part FROM numbers(6)")
    finally:
        broken_s3.reset()
    assert error, "the INSERT succeeded although the commit upload was reset"
    logging.info("commit and cleanup failed: %s", error.splitlines()[0][:200])
    assert log_versions(started_cluster, path) == [0, 1]
    assert node.query(f"SELECT count() FROM {path}").strip() == "6"
    # Orphans, if any, are exactly the files of the failed INSERT and none is referenced by the log.
    orphans = _all_parquet_objects(started_cluster, path) - _committed_add_objects(started_cluster, path)
    assert len(orphans) <= data_files_per_insert, orphans
    node.query(f"INSERT INTO {path} SELECT number + 200 AS id, number % 2 AS part FROM numbers(6)")
    assert log_versions(started_cluster, path) == [0, 1, 2]
    assert node.query(f"SELECT count() FROM {path}").strip() == "12"
    node.query(f"DROP TABLE {path}")


@pytest.mark.parametrize("partitioned", [False, True])
def test_concurrent_writers_through_slow_s3(started_cluster, partitioned):
    node = started_cluster.instances["node1"]
    broken_s3 = started_cluster.broken_s3
    path = _new_mock_table(started_cluster, node, "test_slow_concurrent", partitioned)
    broken_s3.reset()
    broken_s3.setup_slow_answers(minimal_length=0, timeout=1, probability=0.3)
    writers = 5
    barrier = threading.Barrier(writers)
    outcomes = [None] * writers

    def writer(i):
        barrier.wait()
        try:
            node.query(f"INSERT INTO {path} SELECT number + {(i + 1) * 100} AS id, number % 2 AS part FROM numbers(6)")
            outcomes[i] = "ok"
        except Exception as e:  # pylint: disable=broad-except
            outcomes[i] = str(e)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(writers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    broken_s3.reset()
    for o in outcomes:
        if o != "ok":
            assert "commit conflict at version" in o, o
    successes = sum(1 for o in outcomes if o == "ok")
    _assert_consistent(started_cluster, node, path, 1 + successes, partitioned)
    node.query(f"DROP TABLE {path}")
