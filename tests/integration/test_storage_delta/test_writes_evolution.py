"""
Delta Lake writes across schema evolution, Spark maintenance, cluster table functions,
grants and named collections, mixed ClickHouse versions, and a bounded differential run
against Spark as a second writer.
"""

import datetime
import decimal
import logging
import random

import pyarrow as pa
import pyspark
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.s3_tools import LocalDownloader, LocalUploader
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config
from test_storage_delta.test import (
    create_empty_delta_table,
    delta_engine_definition,
    list_delta_data_files,
    randomize_table_name,
)

USER_FILES = "/var/lib/clickhouse/user_files"

cluster = ClickHouseCluster(__file__, with_spark=True)


def get_spark(started_cluster):
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_writes_evolution")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.warehouse", USER_FILES)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}/",
        )
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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
        for name in ("node1", "node2"):
            cluster.add_instance(
                name,
                main_configs=[
                    "configs/config.d/named_collections.xml",
                    "configs/config.d/remote_servers.xml",
                ],
                user_configs=[
                    "configs/users.d/users.xml",
                    "configs/users.d/enable_writes.xml",
                    "configs/users.d/access_management.xml",
                ],
                with_minio=True,
                stay_alive=True,
            )
        # A replica where the operator did not enable writes and pinned the feature tier to production:
        # the shape Cloud customers hit when only some replicas of a service carry the profile change.
        cluster.add_instance(
            "node_production_only",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/allow_feature_tier_production.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node_old_writes",
            main_configs=["configs/config.d/named_collections.xml"],
            user_configs=["configs/users.d/users.xml", "configs/users.d/enable_writes_old.xml"],
            image="clickhouse/clickhouse-server",
            tag="26.6",
            with_minio=True,
            with_installed_binary=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        if int(cluster.instances["node1"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip("DeltaLake engine is not available")
        cluster.spark_session = ResilientSparkSession(lambda: get_spark(cluster))
        yield cluster
    finally:
        cluster.shutdown()


def push_to_node(node, path):
    LocalUploader(node).upload_directory(f"{path}/", f"{path}/")


def pull_from_node(node, path):
    LocalDownloader(node).download_directory(f"{path}/", f"{path}/")


def s3_path(started_cluster, path):
    return f"s3a://{started_cluster.minio_bucket}/{path}"


def spark_rows(spark, location, order_by="id"):
    return [tuple(r) for r in spark.read.format("delta").load(location).orderBy(order_by).collect()]


def log_versions(started_cluster, path):
    return sorted(
        int(obj.object_name.rsplit("/", 1)[1][:-5])
        for obj in started_cluster.minio_client.list_objects(started_cluster.minio_bucket, f"{path}/_delta_log/", recursive=True)
        if obj.object_name.endswith(".json")
    )


# ---------------------------------------------------------------------------------------------
# schema evolution
# ---------------------------------------------------------------------------------------------


def test_spark_add_column_then_clickhouse_append(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_evolution")
    path = f"{USER_FILES}/{table_name}"
    table = f"delta.`{path}`"

    spark.sql(f"CREATE TABLE {table} (id INT, v STRING) USING delta")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'spark')")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN extra INT")
    spark.sql(f"INSERT INTO {table} VALUES (2, 'spark', 20)")
    push_to_node(node, path)

    # A ClickHouse table attached after the evolution sees and writes the new column.
    node.query(f"CREATE TABLE {table_name} ENGINE = DeltaLakeLocal('{path}')")
    assert node.query(f"SELECT name FROM system.columns WHERE table = '{table_name}' ORDER BY position FORMAT TSV") == "id\nv\nextra\n"
    node.query(f"INSERT INTO {table_name} VALUES (3, 'clickhouse', 30)")
    pull_from_node(node, path)
    assert spark_rows(spark, path) == [(1, "spark", None), (2, "spark", 20), (3, "clickhouse", 30)]

    # Spark keeps appending after the ClickHouse commit.
    spark.sql(f"INSERT INTO {table} VALUES (4, 'spark', 40)")
    push_to_node(node, path)
    assert node.query(f"SELECT id, v, extra FROM {table_name} ORDER BY id FORMAT TSV") == "1\tspark\t\\N\n2\tspark\t20\n3\tclickhouse\t30\n4\tspark\t40\n"


def test_write_with_stale_declared_schema_and_pinned_snapshot(started_cluster):
    """A ClickHouse table declared before Spark added a column, and an INSERT pinned to the
    pre-evolution snapshot: both are rejected (the write schema always comes from the latest
    snapshot), and whatever were accepted would have to read back consistently in Spark."""
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_stale_schema")
    path = f"{USER_FILES}/{table_name}"
    table = f"delta.`{path}`"

    spark.sql(f"CREATE TABLE {table} (id INT, v STRING) USING delta")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'spark')")  # version 1
    push_to_node(node, path)
    node.query(f"CREATE TABLE {table_name} (id Int32, v String) ENGINE = DeltaLakeLocal('{path}')")

    spark.sql(f"ALTER TABLE {table} ADD COLUMN extra INT")  # version 2
    push_to_node(node, path)

    outcomes = {}
    for label, settings in (("stale_declared_schema", ""), ("pinned_snapshot", "SETTINGS delta_lake_snapshot_version = 1")):
        _, error = node.query_and_get_answer_with_error(f"INSERT INTO {table_name} (id, v) VALUES (2, 'clickhouse') {settings}")
        outcomes[label] = "rejected" if error else "committed"
        if error:
            assert "INCOMPATIBLE_COLUMNS" in error or "DELTA_KERNEL_ERROR" in error, error
    logging.info("outcomes: %s", outcomes)
    assert outcomes == {"stale_declared_schema": "rejected", "pinned_snapshot": "rejected"}, outcomes
    pull_from_node(node, path)

    rows = spark_rows(spark, path)
    committed = sum(1 for o in outcomes.values() if o == "committed")
    # Whatever was accepted must be readable by Spark with the new column as NULL; nothing else.
    assert rows == [(1, "spark", None)] + [(2, "clickhouse", None)] * committed, rows
    history = spark.sql(f"DESCRIBE HISTORY {table}").count()
    assert history == 3 + committed
    fresh = randomize_table_name("test_stale_schema_fresh")
    node.query(f"CREATE TABLE {fresh} ENGINE = DeltaLakeLocal('{path}')")
    assert node.query(f"SELECT count() FROM {fresh}").strip() == str(1 + committed)


# ---------------------------------------------------------------------------------------------
# Spark maintenance over ClickHouse-written files
# ---------------------------------------------------------------------------------------------


def test_spark_optimize_and_zorder_after_clickhouse_writes(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = randomize_table_name("test_optimize")
    path = f"{USER_FILES}/{table_name}"
    table = f"delta.`{path}`"

    spark.sql(f"CREATE TABLE {table} (id INT, v STRING) USING delta")
    push_to_node(node, path)
    node.query(f"CREATE TABLE {table_name} (id Int32, v String) ENGINE = DeltaLakeLocal('{path}')")
    for i in range(5):
        node.query(f"INSERT INTO {table_name} SELECT number + {i * 10}, toString(number) FROM numbers(10)")
    pull_from_node(node, path)
    assert spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0].c == 50

    spark.sql(f"OPTIMIZE {table}")
    spark.sql(f"OPTIMIZE {table} ZORDER BY (id)")
    spark.sql(f"VACUUM {table} RETAIN 0 HOURS")
    push_to_node(node, path)

    # ClickHouse reads the compacted table and appends to it.
    fresh = randomize_table_name("test_optimize_fresh")
    node.query(f"CREATE TABLE {fresh} (id Int32, v String) ENGINE = DeltaLakeLocal('{path}')")
    assert node.query(f"SELECT count(), uniqExact(id) FROM {fresh}").strip() == "50\t50"
    node.query(f"INSERT INTO {fresh} SELECT number + 50, toString(number) FROM numbers(10)")
    pull_from_node(node, path)
    assert spark.sql(f"SELECT count(*) AS c, count(DISTINCT id) AS d FROM {table}").collect()[0][:] == (60, 60)
    ops = [r.operation for r in spark.sql(f"DESCRIBE HISTORY {table}").collect()]
    assert "OPTIMIZE" in ops and "VACUUM END" in ops, ops


# ---------------------------------------------------------------------------------------------
# cluster table function, grants, named collections
# ---------------------------------------------------------------------------------------------


def test_writes_gate_differs_per_replica(started_cluster):
    """Two replicas of one table, writes enabled on one only, and the Beta tier locked on the other:
    every rejection is deterministic and names what to change, nothing is half-committed."""
    node_on = started_cluster.instances["node1"]
    node_off = started_cluster.instances["node_production_only"]
    path = randomize_table_name("test_gate_per_replica")
    create_empty_delta_table(started_cluster, "s3", path, pa.schema([("id", pa.int32(), False)]))
    for node in (node_on, node_off):
        node.query(f"CREATE TABLE {path} (id Int32) ENGINE = {delta_engine_definition(started_cluster, 's3', path)}")

    assert node_off.query("SELECT value FROM system.server_settings WHERE name = 'allow_feature_tier'").strip() == "3"
    assert node_off.query("SELECT value FROM system.settings WHERE name = 'allow_delta_lake_writes'").strip() == "0"

    # The replica without the setting rejects the INSERT and tells the user which setting to change.
    _, error = node_off.query_and_get_answer_with_error(f"INSERT INTO {path} VALUES (1)")
    assert "SUPPORT_IS_DISABLED" in error and "allow_delta_lake_writes" in error, error
    # ...and the production-only tier stops the user from turning the Beta setting on by any spelling.
    for query in (
        "SET allow_delta_lake_writes = 1",
        "SET allow_experimental_delta_lake_writes = 1",
        f"INSERT INTO {path} SETTINGS allow_delta_lake_writes = 1 VALUES (1)",
    ):
        _, error = node_off.query_and_get_answer_with_error(query)
        assert "READONLY" in error and "allow_feature_tier" in error, (query, error)
    # The Experimental CREATE setting is locked the same way.
    _, error = node_off.query_and_get_answer_with_error("SET allow_delta_lake_create_table = 1")
    assert "READONLY" in error and "allow_feature_tier" in error, error
    assert log_versions(started_cluster, path) == [0]
    assert list_delta_data_files(started_cluster, "s3", path) == []

    # The enabled replica writes; both replicas read the same table afterwards.
    node_on.query(f"INSERT INTO {path} VALUES (1), (2)")
    assert log_versions(started_cluster, path) == [0, 1]
    for node in (node_on, node_off):
        assert node.query(f"SELECT count() FROM {path}").strip() == "2"
        node.query(f"DROP TABLE {path}")


def test_delta_lake_cluster_table_function_insert(started_cluster):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]
    path = randomize_table_name("test_cluster_insert")
    create_empty_delta_table(started_cluster, "s3", path, pa.schema([("id", pa.int32(), False)]))
    url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{path}/"

    node1.query(f"INSERT INTO TABLE FUNCTION deltaLakeCluster('cluster', '{url}', 'minio', '{minio_secret_key}') SELECT number AS id FROM numbers(10)")
    # Exactly one commit, visible from every node, through the plain and the cluster function.
    assert log_versions(started_cluster, path) == [0, 1]
    for node in (node1, node2):
        assert node.query(f"SELECT count() FROM deltaLake('{url}', 'minio', '{minio_secret_key}')").strip() == "10"
    assert node2.query(f"SELECT count() FROM deltaLakeCluster('cluster', '{url}', 'minio', '{minio_secret_key}')").strip() == "10"


def test_grants_and_named_collection(started_cluster):
    node = started_cluster.instances["node1"]
    path = randomize_table_name("test_grants")
    create_empty_delta_table(started_cluster, "s3", path, pa.schema([("id", pa.int32(), False)]))
    url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{path}/"
    user = f"writer_{path}"
    nc = f"nc_{path}"
    node.query(f"CREATE USER {user} IDENTIFIED WITH no_password SETTINGS allow_delta_lake_writes = 1")
    node.query(f"CREATE NAMED COLLECTION {nc} AS url = '{url}', access_key_id = 'minio', secret_access_key = '{minio_secret_key}'")
    node.query(f"CREATE TABLE {path} (id Int32) ENGINE = DeltaLake({nc})")

    def as_user(query):
        return node.query_and_get_answer_with_error(query, user=user)

    # No grants: neither the table nor the table function nor the collection is writable.
    for query in (
        f"INSERT INTO {path} VALUES (1)",
        f"INSERT INTO TABLE FUNCTION deltaLake('{url}', 'minio', '{minio_secret_key}') VALUES (1)",
        f"INSERT INTO TABLE FUNCTION deltaLake({nc}) VALUES (1)",
    ):
        _, error = as_user(query)
        assert "ACCESS_DENIED" in error, (query, error)
    assert log_versions(started_cluster, path) == [0]
    assert list_delta_data_files(started_cluster, "s3", path) == []

    # INSERT on the table is enough for the engine table.
    node.query(f"GRANT INSERT ON default.{path} TO {user}")
    _, error = as_user(f"INSERT INTO {path} VALUES (1)")
    assert not error, error
    # The table function needs the S3 source grant and CREATE TEMPORARY TABLE, the collection its own grant.
    node.query(f"GRANT S3 ON *.* TO {user}")
    node.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    _, error = as_user(f"INSERT INTO TABLE FUNCTION deltaLake('{url}', 'minio', '{minio_secret_key}') VALUES (2)")
    assert not error, error
    _, error = as_user(f"INSERT INTO TABLE FUNCTION deltaLake({nc}) VALUES (3)")
    assert "ACCESS_DENIED" in error, error
    node.query(f"GRANT NAMED COLLECTION ON {nc} TO {user}")
    _, error = as_user(f"INSERT INTO TABLE FUNCTION deltaLake({nc}) VALUES (3)")
    assert not error, error

    assert log_versions(started_cluster, path) == [0, 1, 2, 3]
    assert node.query(f"SELECT id FROM {path} ORDER BY id FORMAT TSV") == "1\n2\n3\n"
    # The collection's secret never appears in the table's definition or in query_log
    # (compared in Python: a query text containing it would itself be a leak).
    node.query("SYSTEM FLUSH LOGS")
    assert minio_secret_key not in node.query(f"SHOW CREATE TABLE {path}")
    logged = node.query(f"SELECT query FROM system.query_log WHERE query LIKE '%{path}%' FORMAT TSVRaw")
    assert minio_secret_key not in logged
    node.query(f"DROP TABLE {path}")
    node.query(f"DROP NAMED COLLECTION {nc}")
    node.query(f"DROP USER {user}")


# ---------------------------------------------------------------------------------------------
# mixed versions
# ---------------------------------------------------------------------------------------------


def test_mixed_version_writers_share_a_table(started_cluster):
    """A 26.6 server and the current one append to the same table in turns (Cloud rolling
    upgrade): commits interleave, every reader sees every row, and Spark reads the result."""
    new = started_cluster.instances["node1"]
    old = started_cluster.instances["node_old_writes"]
    spark = started_cluster.spark_session
    path = randomize_table_name("test_mixed_versions")
    schema = pa.schema([("id", pa.int32(), False), ("part", pa.string(), False)])
    create_empty_delta_table(started_cluster, "s3", path, schema, partition_by=["part"])
    engine = delta_engine_definition(started_cluster, "s3", path)
    for node in (new, old):
        node.query(f"CREATE TABLE {path} (id Int32, part String) ENGINE = {engine}")

    total = 0
    for i, node in enumerate((old, new, old, new)):
        node.query(f"INSERT INTO {path} SELECT number + {i * 100}, 'p{i % 2}' FROM numbers(10)")
        total += 10
        for reader in (new, old):
            assert reader.query(f"SELECT count() FROM {path}").strip() == str(total), (i, reader.name)

    assert log_versions(started_cluster, path) == [0, 1, 2, 3, 4]
    assert len(list_delta_data_files(started_cluster, "s3", path)) == 4
    rows = spark_rows(spark, s3_path(started_cluster, path))
    assert len(rows) == 40 and len({r[0] for r in rows}) == 40
    assert {r[1] for r in rows} == {"p0", "p1"}
    for node in (new, old):
        node.query(f"DROP TABLE {path}")


# ---------------------------------------------------------------------------------------------
# differential: ClickHouse and Spark write the same random rows into twin tables
# ---------------------------------------------------------------------------------------------

TYPE_POOL = {
    "INT": ("Int32", lambda rng: rng.randint(-(2**31), 2**31 - 1)),
    "BIGINT": ("Int64", lambda rng: rng.randint(-(2**63), 2**63 - 1)),
    "DOUBLE": ("Float64", lambda rng: round(rng.uniform(-1e6, 1e6), 6)),
    "STRING": ("String", lambda rng: "".join(rng.choice("abc xyz/%=日本-_") for _ in range(rng.randint(0, 12)))),
    "BOOLEAN": ("Bool", lambda rng: rng.choice([True, False])),
    "DATE": ("Date32", lambda rng: datetime.date(1970, 1, 1) + datetime.timedelta(days=rng.randint(0, 30000))),
    "TIMESTAMP": ("DateTime64(6)", lambda rng: datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=rng.randint(0, 10**9), microseconds=rng.randint(0, 999999))),
    "DECIMAL(12,3)": ("Decimal(12, 3)", lambda rng: decimal.Decimal(rng.randint(-(10**11), 10**11)) / 1000),
}
PARTITIONABLE = ["INT", "BOOLEAN", "DATE"]


def sql_literal(value, spark_type, for_spark):
    if value is None:
        return f"CAST(NULL AS {spark_type})" if for_spark else "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, decimal.Decimal):
        return f"CAST('{value}' AS {spark_type})" if for_spark else f"toDecimal64('{value}', 3)"
    if isinstance(value, datetime.datetime):
        text = value.isoformat(sep=" ")
        return f"TIMESTAMP '{text}'" if for_spark else f"'{text}'"
    if isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'" if for_spark else f"'{value.isoformat()}'"
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


@pytest.mark.parametrize("seed", range(6))
def test_differential_random_table_vs_spark(started_cluster, seed):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    rng = random.Random(seed)
    n_cols = rng.randint(1, 5)
    columns = [(f"c{i}", rng.choice(sorted(TYPE_POOL))) for i in range(n_cols)]
    partition = None
    if n_cols > 1 and rng.random() < 0.5:
        candidates = [name for name, t in columns if t in PARTITIONABLE]
        if candidates:
            partition = rng.choice(candidates)
    rows = [[None if (rng.random() < 0.1 and name != partition) else TYPE_POOL[t][1](rng) for name, t in columns] for _ in range(rng.randint(1, 60))]
    logging.info("seed %s: columns=%s partition=%s rows=%s", seed, columns, partition, len(rows))

    ddl_cols = ", ".join(f"{name} {t}" for name, t in columns)
    partitioned_by = f" PARTITIONED BY ({partition})" if partition else ""
    paths = {}
    for who in ("clickhouse", "spark"):
        table_name = randomize_table_name(f"test_diff_{seed}_{who}")
        path = f"{USER_FILES}/{table_name}"
        spark.sql(f"CREATE TABLE delta.`{path}` ({ddl_cols}) USING delta{partitioned_by}")
        paths[who] = (table_name, path)

    ch_name, ch_path = paths["clickhouse"]
    push_to_node(node, ch_path)
    node.query(f"CREATE TABLE {ch_name} ({', '.join(f'{n} Nullable({TYPE_POOL[t][0]})' for n, t in columns)}) ENGINE = DeltaLakeLocal('{ch_path}')")
    node.query(f"INSERT INTO {ch_name} VALUES " + ", ".join("(" + ", ".join(sql_literal(v, t, False) for v, (_, t) in zip(row, columns)) + ")" for row in rows))
    pull_from_node(node, ch_path)

    _, spark_path = paths["spark"]
    spark.sql(f"INSERT INTO delta.`{spark_path}` VALUES " + ", ".join("(" + ", ".join(sql_literal(v, t, True) for v, (_, t) in zip(row, columns)) + ")" for row in rows))

    def read(path):
        df = spark.read.format("delta").load(path)
        return sorted(tuple(str(v) for v in r) for r in df.collect())

    assert read(ch_path) == read(spark_path)
    assert node.query(f"SELECT count() FROM {ch_name}").strip() == str(len(rows))
