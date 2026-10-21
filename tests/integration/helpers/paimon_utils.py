"""Helpers for Paimon integration tests over object storage.

Mirrors helpers/iceberg_utils.py: test data is written locally by a Spark
session with the Paimon catalog, then the table directory is uploaded to
MinIO / Azurite. Paimon table directories are
relocatable (metadata stores relative paths), which is the same property the
stateless data_minio/paimon_* fixtures rely on.

Uploads happen in commit order (data/schemas -> manifests -> snapshots ->
hint files last), matching how real Paimon writers commit, so a concurrent
reader never observes a snapshot whose referenced files are missing. They are
also append-only: every object except the `LATEST`/`EARLIEST` hints is
immutable once committed and is therefore uploaded only once.
"""

import hashlib
import os
import time
import uuid
from contextlib import contextmanager

import pyspark

from helpers.iceberg_utils import (  # noqa: F401  (re-exported for test modules)
    get_uuid_str,
)
from helpers.spark_tools import write_spark_log_config

PAIMON_WAREHOUSE = "/var/lib/clickhouse/user_files/paimon_data"
PAIMON_CATALOG = "paimon"

# Shared Spark-DDL type matrix and its expected ClickHouse mapping — asserted
# identically by the emulator suite (test_storage_paimon_with_spark) and the
# real-cloud e2e suite (test_e2e_paimon), so the two tiers cannot drift.
# Spark TIMESTAMP has "with local time zone" semantics (-> DateTime64(6)),
# TIMESTAMP_NTZ maps to Paimon TIMESTAMP (-> DateTime64(6, 'UTC')).
# f_array/f_map are NOT NULL because a *nullable* Paimon ARRAY/MAP column maps
# to Nullable(Array(...))/Nullable(Map(...)), which ClickHouse rejects
# (see https://github.com/ClickHouse/ClickHouse/issues/113337).
TYPES_COLUMNS_SQL = """
    f_boolean BOOLEAN,
    f_tinyint TINYINT,
    f_smallint SMALLINT,
    f_int INT,
    f_bigint BIGINT,
    f_float FLOAT,
    f_double DOUBLE,
    f_decimal DECIMAL(9, 1),
    f_decimal2 DECIMAL(18, 1),
    f_decimal3 DECIMAL(38, 1),
    f_char CHAR(1),
    f_varchar VARCHAR(8),
    f_string STRING,
    f_binary BINARY,
    f_date DATE,
    f_timestamp_ltz TIMESTAMP,
    f_timestamp TIMESTAMP_NTZ,
    f_array ARRAY<INT> NOT NULL,
    f_map MAP<STRING, INT> NOT NULL,
    f_int_nn INT NOT NULL
"""

TYPES_EXPECTED_SCHEMA = [
    ["f_boolean", "Nullable(Bool)"],
    ["f_tinyint", "Nullable(Int8)"],
    ["f_smallint", "Nullable(Int16)"],
    ["f_int", "Nullable(Int32)"],
    ["f_bigint", "Nullable(Int64)"],
    ["f_float", "Nullable(Float32)"],
    ["f_double", "Nullable(Float64)"],
    ["f_decimal", "Nullable(Decimal(9, 1))"],
    ["f_decimal2", "Nullable(Decimal(18, 1))"],
    ["f_decimal3", "Nullable(Decimal(38, 1))"],
    ["f_char", "Nullable(FixedString(1))"],
    ["f_varchar", "Nullable(String)"],
    ["f_string", "Nullable(String)"],
    ["f_binary", "Nullable(String)"],
    ["f_date", "Nullable(Date)"],
    ["f_timestamp_ltz", "Nullable(DateTime64(6))"],
    ["f_timestamp", "Nullable(DateTime64(6, \\'UTC\\'))"],
    ["f_array", "Array(Nullable(Int32))"],
    ["f_map", "Map(String, Nullable(Int32))"],
    ["f_int_nn", "Int32"],
]

TYPES_VALUES_ROW = (
    "(true, 1, 2, 3, 4, 5.5, 6.25, 7.7, 8.8, 9.9, 'c', 'varchar', 'string', "
    "CAST('ab' AS BINARY), DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00', "
    "TIMESTAMP_NTZ '2024-01-15 11:30:00', array(1, 2), map('k', 1), 100)"
)
TYPES_NULLS_ROW = (
    "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
    "NULL, NULL, NULL, NULL, array(), map(), 200)"
)

TYPES_EXPECTED_DATA = [
    [
        "true",  # BOOLEAN maps to Bool, which renders as true/false rather than 1/0
        "1",
        "2",
        "3",
        "4",
        "5.5",
        "6.25",
        "7.7",
        "8.8",
        "9.9",
        "c",
        "varchar",
        "string",
        "ab",
        "2024-01-15",
        "2024-01-15 10:30:00.000000",
        "2024-01-15 11:30:00.000000",
        "[1,2]",
        "{'k':1}",
        "100",
    ],
    ["\\N"] * 17 + ["[]", "{}", "200"],
]


def paimon_table_dir(table_name):
    """Table directory relative to user_files (SparkCatalog layout is <warehouse>/<db>.db/<table>)."""
    return f"/paimon_data/default.db/{table_name}/"


def spark_table(table_name, catalog=PAIMON_CATALOG):
    return f"{catalog}.default.{table_name}"


def get_spark_paimon(log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder.appName("paimon_utils")
        .config(
            f"spark.sql.catalog.{PAIMON_CATALOG}",
            "org.apache.paimon.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{PAIMON_CATALOG}.warehouse",
            f"file:{PAIMON_WAREHOUSE}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        # Deterministic TIMESTAMP (with local time zone) literals regardless of host TZ.
        .config("spark.sql.session.timeZone", "UTC")
        .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.getOrCreate()


@contextmanager
def drop_tables_at_exit(node, *table_names):
    """Guarantee DROP TABLE ... SYNC on every exit path: persistent Paimon
    tables keep a background metadata-refresh task and Keeper state alive on
    the shared package-scoped cluster if a failing test leaks them."""
    try:
        yield
    finally:
        for table_name in reversed(table_names):
            node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")


def wait_until_query_result(
    node, query, expected, retries=60, sleep_seconds=0.5, settings=None
):
    """Poll until the query returns the expected result (Paimon metadata refresh is asynchronous)."""
    last_result = ""
    for _ in range(retries):
        last_result = node.query(query, settings=settings)
        if last_result == expected:
            return
        time.sleep(sleep_seconds)
    raise AssertionError(
        f"Unexpected result for query: {query}\nExpected: {expected!r}\nActual: {last_result!r}"
    )


def parse_tsv(raw):
    return [line.split("\t") for line in raw.strip().split("\n") if line]


def id_array_str(ids):
    """The exact TSV form ClickHouse prints for arraySort(groupArray(id)):
    [1,2,3] with no spaces, trailing newline; [] for an empty table."""
    return "[" + ",".join(str(i) for i in ids) + "]\n"


def query_profile_events(instance, query, events, settings=None):
    """Run the query under a unique query_id and return the listed ProfileEvents
    values (as ints, in the given order) from system.query_log."""
    query_id = f"paimon-events-{uuid.uuid4()}"
    instance.query(query, query_id=query_id, settings=settings)
    instance.query("SYSTEM FLUSH LOGS")
    events_sql = ", ".join(f"ProfileEvents['{event}']" for event in events)
    return [
        int(value)
        for value in instance.query(
            f"SELECT {events_sql} FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        ).split()
    ]


def assert_cluster_fan_out(remote_node, query_id):
    """Prove a `paimon*Cluster` table function actually fanned out: the remote
    replica must have started a secondary (non-initial) query for this
    query_id. Result equality with the local table function alone cannot
    distinguish a real fan-out from a silent fallback to the initiator-only
    path — both read the same shared object storage."""
    remote_node.query("SYSTEM FLUSH LOGS")
    secondary_queries = int(
        remote_node.query(
            f"SELECT count() FROM system.query_log "
            f"WHERE initial_query_id = '{query_id}' AND NOT is_initial_query "
            f"AND type = 'QueryStart'"
        )
    )
    assert secondary_queries > 0, (
        f"no secondary query on {remote_node.name} for initial_query_id "
        f"{query_id}: the cluster function did not fan out"
    )


def get_schema_and_data(instance, table_expression, order_by="ALL", settings=None):
    """DESC (name, type only) + full data, both as lists of rows, with optional SELECT settings."""
    schema = [
        row[:2] for row in parse_tsv(instance.query(f"DESC {table_expression}"))
    ]
    data = parse_tsv(
        instance.query(
            f"SELECT * FROM {table_expression} ORDER BY {order_by}", settings=settings
        )
    )
    return schema, data


def create_spark_paimon_table(
    spark, table_name, columns_sql, partitioned_by="", tblproperties=None, catalog=PAIMON_CATALOG
):
    """CREATE a Paimon table via Spark.

    Defaults to append-only; callers may pass 'primary-key' via tblproperties
    (the strict-xfail PK test does), but reader tests must not rely on PK merge
    semantics: ClickHouse reads Paimon tables as a raw union of data files
    without merge-on-read (ClickHouse#113360).
    """
    properties = {"file.format": "parquet"}
    if tblproperties:
        properties.update(tblproperties)
    properties_sql = ", ".join(f"'{k}'='{v}'" for k, v in properties.items())
    partitioned_sql = f"PARTITIONED BY ({partitioned_by})" if partitioned_by else ""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.default")
    spark.sql(
        f"CREATE TABLE {spark_table(table_name, catalog)} ({columns_sql}) "
        f"{partitioned_sql} TBLPROPERTIES ({properties_sql})"
    )


def compact_paimon_table(spark, table_name, catalog=PAIMON_CATALOG):
    """CALL sys.compact on the table and assert a COMPACT snapshot was produced."""
    spark.sql(f"CALL {catalog}.sys.compact(table => 'default.{table_name}')")
    commit_kinds = {
        row.commit_kind
        for row in spark.sql(
            f"SELECT commit_kind FROM {catalog}.default.`{table_name}$snapshots`"
        ).collect()
    }
    assert "COMPACT" in commit_kinds, f"no COMPACT snapshot produced: {commit_kinds}"


def is_paimon_hint_file(rel_path):
    """A snapshot hint (`LATEST`/`EARLIEST`) — the only files a Paimon writer
    rewrites in place; every other object is immutable once committed."""
    return (
        "snapshot" in rel_path.split(os.sep)
        and os.path.basename(rel_path) in ("LATEST", "EARLIEST")
    )


def paimon_upload_rank(rel_path):
    """Commit-order rank of a file within a Paimon table directory.

    `snapshot-N` files carry a numeric secondary key: plain path order is
    lexicographic (`snapshot-10` before `snapshot-2`), and a concurrent
    reader listing snapshots mid-upload would treat the missing intermediate
    ones as compacted-away."""
    parts = rel_path.split(os.sep)
    name = os.path.basename(rel_path)
    if "snapshot" in parts:
        if is_paimon_hint_file(rel_path):
            return (4, 0)
        suffix = name.removeprefix("snapshot-")
        return (3, int(suffix)) if suffix.isdigit() else (3, 0)
    if "manifest" in parts:
        return (2, 0)
    return (1, 0)  # data files and schemas carry no references to other files


def upload_paimon_dir(local_dir, upload_file_fn, remote_digest_fn=None):
    """Walk local_dir and call upload_file_fn(local_file, rel_path) for every
    file, in commit order (see module docstring): data/schemas -> manifests ->
    snapshots -> hint files last, per paimon_upload_rank.

    remote_digest_fn(local_file, rel_path) -> md5 hex of the already-uploaded
    object's content, or None when it is absent. When supplied, a file whose
    content is already there byte for byte is not re-uploaded, which keeps its
    storage-side generation token stable (see upload_paimon_table). Content
    rather than size is compared because a table dropped and recreated at the
    same path writes a fresh `schema/schema-0` of identical length, and skipping
    that would serve the new table from the old table's metadata cache entry.
    Hint files are always uploaded, matching a real writer, which rewrites
    exactly those. Without remote_digest_fn every file is uploaded.
    """
    files = []
    for root, _dirs, names in os.walk(local_dir):
        for name in names:
            local_file = os.path.join(root, name)
            rel_path = os.path.relpath(local_file, local_dir)
            files.append((paimon_upload_rank(rel_path), local_file, rel_path))
    if not files:
        raise RuntimeError(f"No files found to upload in {local_dir}")
    for _rank, local_file, rel_path in sorted(files):
        if remote_digest_fn is not None and not is_paimon_hint_file(rel_path):
            if remote_digest_fn(local_file, rel_path) == _file_digest(local_file):
                continue
        upload_file_fn(local_file, rel_path)


def _file_digest(local_file):
    with open(local_file, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def _remote_digest_fn(started_cluster, storage_type):
    """md5 hex of an already-uploaded object's content, or None when absent.

    Reads the remote bytes back rather than comparing a stored checksum: an
    Azure ETag is a generation token, and an S3 ETag is the content md5 only for
    a single-part upload. These objects are a few hundred bytes each.

    The lookup passes the very key upload_paimon_table uploads under, through
    the same client, so backend-specific key normalization cannot diverge
    between the two (MinIO strips a leading '/', Azurite keeps it).
    """
    if storage_type == "s3":
        client = started_cluster.minio_client
        bucket = started_cluster.minio_bucket

        def remote_digest(local_file, _rel_path):
            try:
                response = client.get_object(bucket, local_file)
            except Exception as e:
                if getattr(e, "code", None) in ("NoSuchKey", "NoSuchObject"):
                    return None
                raise
            try:
                return hashlib.md5(response.read()).hexdigest()
            finally:
                response.close()
                response.release_conn()

    elif storage_type == "azure":
        container_client = started_cluster.container_client

        def remote_digest(local_file, _rel_path):
            try:
                blob_client = container_client.get_blob_client(local_file)
                return hashlib.md5(blob_client.download_blob().readall()).hexdigest()
            except Exception as e:
                if getattr(e, "status_code", None) == 404:
                    return None
                raise

    else:
        raise Exception(f"Unknown paimon storage type: {storage_type}")

    return remote_digest


def upload_paimon_table(started_cluster, storage_type, table_name):
    """Upload the table directory in commit order (see module docstring),
    uploading each immutable object at most once.

    Remote object names equal the absolute local paths, the same naming
    CloudUploader.upload_directory produces, so DDL paths are unchanged.

    Re-uploading an object that is already present is not a no-op remotely: an
    Azure Blob ETag is a per-generation token rather than a content hash, so a
    byte-identical re-upload still mints a new one and a reader holding the old
    one fails with FILE_CHANGED_DURING_READ. Real Paimon writers only ever add
    objects, so uploading each of them once is also the faithful emulation.
    """
    uploader = getattr(started_cluster, f"default_{storage_type}_uploader")
    local_dir = "/var/lib/clickhouse/user_files" + paimon_table_dir(table_name)
    upload_paimon_dir(
        local_dir,
        lambda local_file, _rel_path: uploader.upload_file(local_file, local_file),
        remote_digest_fn=_remote_digest_fn(started_cluster, storage_type),
    )


def execute_spark_query(started_cluster, storage_type, table_name, query):
    """Run one Spark SQL statement against the Paimon catalog, then re-upload the table."""
    started_cluster.spark_session.sql(query)
    upload_paimon_table(started_cluster, storage_type, table_name)


def get_creation_expression(
    storage_type,
    table_name,
    cluster,
    table_function=False,
    run_on_cluster=False,
    additional_settings=None,
    engine_name=None,
):
    """DDL / table-function expression for reading a Paimon table from ClickHouse.

    additional_settings go into the CREATE TABLE SETTINGS clause
    (DataLakeStorageSettings, e.g. paimon_incremental_read).
    engine_name overrides the default engine, e.g. "Paimon" to test the
    disk-autodetect engine (only meaningful with table_function=False).
    """
    table_dir = f"var/lib/clickhouse/user_files/paimon_data/default.db/{table_name}/"
    if storage_type == "s3":
        function, cluster_function, engine = "paimonS3", "paimonS3Cluster", "PaimonS3"
        location = f"s3, filename = '{table_dir}', url = 'http://minio1:9001/{cluster.minio_bucket}/'"
    elif storage_type == "azure":
        function, cluster_function, engine = "paimonAzure", "paimonAzureCluster", "PaimonAzure"
        storage_account_url = cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
        location = (
            f"azure, container = '{cluster.azure_container_name}', "
            f"storage_account_url = '{storage_account_url}', "
            f"blob_path = '/{table_dir}'"
        )
    else:
        raise Exception(f"Unknown paimon storage type: {storage_type}")

    if run_on_cluster:
        assert table_function
        return f"{cluster_function}('cluster_simple', {location})"
    if table_function:
        return f"{function}({location})"
    settings_expression = (
        " SETTINGS " + ", ".join(additional_settings) if additional_settings else ""
    )
    return f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE={engine_name or engine}({location})
        {settings_expression};
        """


def create_paimon_table(
    storage_type, node, table_name, cluster, additional_settings=None, engine_name=None
):
    node.query(
        get_creation_expression(
            storage_type,
            table_name,
            cluster,
            additional_settings=additional_settings,
            engine_name=engine_name,
        ),
        settings={"allow_experimental_paimon_storage_engine": 1},
    )


def prepare_simple_paimon_table(
    started_cluster, storage_type, table_name, with_second_commit=False
):
    """Append-only (a INT NOT NULL, b STRING) table with rows (1,'one'),(2,'two'),
    plus an optional second commit adding (3,'three'), uploaded to storage."""
    spark = started_cluster.spark_session
    create_spark_paimon_table(spark, table_name, "a INT NOT NULL, b STRING")
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (1, 'one'), (2, 'two')")
    if with_second_commit:
        spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (3, 'three')")
    upload_paimon_table(started_cluster, storage_type, table_name)


def incremental_settings(
    table_name, replica_name="r1", keeper_path=None, refresh_interval_sec=1
):
    return [
        "paimon_incremental_read = 1",
        f"paimon_keeper_path = '{keeper_path or '/clickhouse/paimon/' + table_name}'",
        f"paimon_replica_name = '{replica_name}'",
        f"paimon_metadata_refresh_interval_sec = {refresh_interval_sec}",
    ]


def create_incremental_paimon_table(
    started_cluster,
    storage_type,
    table_name,
    tblproperties=None,
    keeper_path=None,
    refresh_interval_sec=1,
):
    """Spark table (id BIGINT, val STRING) with one warm-up commit (schema
    inference needs data), then the ClickHouse engine table with Keeper
    incremental read enabled. Returns the node the table was created on."""
    spark = started_cluster.spark_session
    instance = started_cluster.instances["node1"]
    create_spark_paimon_table(
        spark, table_name, "id BIGINT, val STRING", tblproperties=tblproperties
    )
    spark.sql(f"INSERT INTO {spark_table(table_name)} VALUES (0, 'warmup')")
    upload_paimon_table(started_cluster, storage_type, table_name)
    create_paimon_table(
        storage_type,
        instance,
        table_name,
        started_cluster,
        additional_settings=incremental_settings(
            table_name, keeper_path=keeper_path, refresh_interval_sec=refresh_interval_sec
        ),
    )
    return instance
