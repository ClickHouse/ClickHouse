"""Upgrade and mixed-version checks of data lake objects, against a local REST catalog on MinIO.

The same shapes, without a real catalog service, so they run in every integration job. What needs a real service (Glue,
OneLake, Databricks authentication and their catalog-specific metadata) is checked by a separate E2E suite.

* An old server creates a `DataLakeCatalog` database, an `IcebergS3` table through a named collection, a view and
  materialized views over them, and writes into the Iceberg table. It is then restarted on the new binary with the
  same metadata: everything must load, return the same results, and accept new writes and DDL.
* An old and a new server form one cluster: parallel replicas over a catalog table and `icebergS3Cluster` must
  return the single-node result, an Iceberg table written by one version must be readable by the other, and a
  distributed `INSERT ... SELECT FROM s3Cluster` must apply the `DEFAULT` expressions of the destination.

The old version is the release line that ClickHouse Cloud services upgrade from, taken from the public
`clickhouse/clickhouse-server` image: the private Cloud builds are not published as images, and the data lake code
they run is the public one.
"""

import time
import uuid

import pyarrow as pa
import pytest
import requests
from pyiceberg.catalog import load_catalog

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

# The release line Cloud services upgrade from. Bump it when Cloud moves on.
OLD_VERSION = "26.6"

REST_CATALOG_URL = "http://rest:8181/v1"
WAREHOUSE_BUCKET = "warehouse-rest"
MINIO_URL = "http://minio1:9001"

cluster = ClickHouseCluster(__file__)


def _add_node(name, old):
    kwargs = (
        dict(
            image="clickhouse/clickhouse-server",
            tag=OLD_VERSION,
            stay_alive=True,
            with_installed_binary=True,
        )
        if old
        else {}
    )
    return cluster.add_instance(
        name,
        main_configs=["configs/data_lake_clusters.xml"],
        user_configs=["configs/data_lake_users.xml"],
        with_iceberg_catalog=True,
        **kwargs,
    )


# Starts on the old version and is restarted on the new one by the upgrade test.
node_upgrade = _add_node("node_upgrade", old=True)
# Stay on their versions: the two replicas of the mixed-version cluster.
node_old = _add_node("node_old", old=True)
node_new = _add_node("node_new", old=False)

SALES = pa.table(
    {
        "id": pa.array(range(1, 21), type=pa.int64()),
        "region": pa.array((["East"] * 5 + ["West"] * 5) * 2, type=pa.string()),
        "amount": pa.array([float(i) * 1.25 for i in range(1, 21)], type=pa.float64()),
        "note": pa.array([None if i % 3 else f"n{i}" for i in range(1, 21)], type=pa.string()),
    }
)

PARALLEL_REPLICAS = {
    "enable_parallel_replicas": 1,
    "automatic_parallel_replicas_mode": 0,
    "max_parallel_replicas": 2,
    "cluster_for_parallel_replicas": "mixed_replicas",
    "parallel_replicas_for_cluster_engines": 1,
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def catalog_table(started_cluster):
    """A table written by a foreign writer (`pyiceberg`) into the REST catalog: `(namespace, table, location)`."""
    # The catalog container accepts connections before it serves requests.
    config_url = f"http://localhost:{started_cluster.iceberg_rest_catalog_port}/v1/config"
    deadline = time.monotonic() + 120
    while True:
        try:
            if requests.get(config_url, timeout=2).status_code == 200:
                break
        except requests.exceptions.RequestException:
            pass
        assert time.monotonic() < deadline, f"the REST catalog at {config_url} did not become ready"
        time.sleep(0.5)
    catalog = load_catalog(
        "demo",
        **{
            "uri": f"http://localhost:{started_cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )
    suffix = uuid.uuid4().hex[:8]
    namespace = f"upgrade_{suffix}"
    table = "sales"
    location = f"{WAREHOUSE_BUCKET}/upgrade_{suffix}/sales"
    catalog.create_namespace(namespace)
    catalog.create_table(
        identifier=f"{namespace}.{table}", schema=SALES.schema, location=f"s3://{location}"
    ).append(SALES)
    return namespace, table, location


def _create_catalog_database(node, database):
    node.query(
        f"""
        DROP DATABASE IF EXISTS {database};
        CREATE DATABASE {database} ENGINE = DataLakeCatalog('{REST_CATALOG_URL}', '{minio_access_key}', '{minio_secret_key}')
        SETTINGS catalog_type = 'rest', warehouse = 'demo', storage_endpoint = '{MINIO_URL}/{WAREHOUSE_BUCKET}'
        """
    )


def _queries(table_expression):
    """Reads whose results must not change between versions."""
    return [
        f"SELECT count(), sum(amount), countIf(note IS NULL) FROM {table_expression}",
        f"SELECT region, count(), sum(amount) FROM {table_expression} GROUP BY region ORDER BY region",
        f"SELECT id, region, amount, note FROM {table_expression} WHERE amount > 10 ORDER BY id",
        f"DESCRIBE TABLE {table_expression} SETTINGS describe_compact_output = 1",
    ]


def _expected_sales_rows():
    return "".join(
        f"{row['id']}\t{row['region']}\t{row['amount']:g}\t{row['note'] if row['note'] is not None else chr(92) + 'N'}\n"
        for row in SALES.to_pylist()
        if row["amount"] > 10
    )


def test_upgrade_keeps_data_lake_objects(started_cluster, catalog_table):
    namespace, table, _ = catalog_table
    node = node_upgrade
    suffix = uuid.uuid4().hex[:8]
    database = f"cat_{suffix}"
    catalog_table_sql = f"{database}.`{namespace}.{table}`"
    iceberg_path = f"{MINIO_URL}/iceberg-data/upgrade_{suffix}/"

    # --- The state of a customer on the old version. ---
    _create_catalog_database(node, database)
    node.query(
        f"""
        CREATE NAMED COLLECTION nc_{suffix} AS
            url = '{iceberg_path}', access_key_id = '{minio_access_key}', secret_access_key = '{minio_secret_key}';
        CREATE TABLE ice_{suffix} (id Int64, region String, amount Float64, note Nullable(String)) ENGINE = IcebergS3(nc_{suffix});
        CREATE MATERIALIZED VIEW ice_mv_{suffix} ENGINE = MergeTree ORDER BY id
            AS SELECT id, amount * 2 AS double_amount FROM ice_{suffix};
        INSERT INTO ice_{suffix} VALUES (1, 'East', 1.5, NULL), (2, 'West', 12.5, 'old');
        CREATE VIEW cat_view_{suffix} AS
            SELECT region, sum(amount) AS total FROM {catalog_table_sql} GROUP BY region;
        CREATE MATERIALIZED VIEW cat_mv_{suffix} REFRESH EVERY 1 YEAR ENGINE = MergeTree ORDER BY tuple()
            AS SELECT region, count() AS c, sum(amount) AS total FROM {catalog_table_sql} GROUP BY region;
        SYSTEM WAIT VIEW cat_mv_{suffix};
        """
    )

    checks = _queries(catalog_table_sql) + _queries(f"ice_{suffix}") + [
        f"SELECT * FROM cat_view_{suffix} ORDER BY region",
        f"SELECT * FROM cat_mv_{suffix} ORDER BY region",
        f"SELECT * FROM ice_mv_{suffix} ORDER BY id",
    ]
    before = {query: node.query(query) for query in checks}
    # The old version must read the foreign-written table correctly, or the comparison below means nothing.
    assert node.query(_queries(catalog_table_sql)[2]) == _expected_sales_rows()
    assert before[f"SELECT * FROM ice_mv_{suffix} ORDER BY id"] == "1\t3\n2\t25\n"

    # --- The upgrade: same metadata, new binary. ---
    node.restart_with_latest_version()
    assert node.query("SELECT version()") != node_old.query("SELECT version()")

    # Every object is still there and returns the same results.
    for query in checks:
        assert node.query(query) == before[query], query

    # The objects accept new work on the new version.
    node.query(
        f"""
        INSERT INTO ice_{suffix} VALUES (3, 'East', 13.5, 'new');
        SYSTEM REFRESH VIEW cat_mv_{suffix};
        SYSTEM WAIT VIEW cat_mv_{suffix};
        """
    )
    assert node.query(f"SELECT * FROM ice_mv_{suffix} ORDER BY id") == "1\t3\n2\t25\n3\t27\n"
    assert node.query(f"SELECT count(), sum(amount) FROM ice_{suffix}") == "3\t27.5\n"
    assert node.query(f"SELECT * FROM cat_mv_{suffix} ORDER BY region") == before[f"SELECT * FROM cat_mv_{suffix} ORDER BY region"]

    # DDL on the upgraded server: a second catalog database and a second table over the same named collection.
    _create_catalog_database(node, f"{database}_new")
    assert node.query(f"SELECT count() FROM {database}_new.`{namespace}.{table}`") == "20\n"
    # A table over an existing Iceberg path takes the schema from it: with a column list `CREATE` makes a new table.
    node.query(f"CREATE TABLE ice_reader_{suffix} ENGINE = IcebergS3(nc_{suffix})")
    assert node.query(f"SELECT count() FROM ice_reader_{suffix}") == "3\n"

    # Write interop: the table was written by both versions, the old one must still read it.
    node_old.query(
        f"CREATE TABLE ice_{suffix} "
        f"ENGINE = IcebergS3('{iceberg_path}', '{minio_access_key}', '{minio_secret_key}')"
    )
    assert node_old.query(f"SELECT * FROM ice_{suffix} ORDER BY id") == "1\tEast\t1.5\t\\N\n2\tWest\t12.5\told\n3\tEast\t13.5\tnew\n"

    node.query(
        f"""
        DROP DATABASE {database}_new;
        DROP TABLE ice_reader_{suffix};
        DROP VIEW cat_mv_{suffix};
        DROP VIEW cat_view_{suffix};
        DROP VIEW ice_mv_{suffix};
        DROP TABLE ice_{suffix};
        DROP NAMED COLLECTION nc_{suffix};
        DROP DATABASE {database};
        """
    )
    node_old.query(f"DROP TABLE ice_{suffix}")


def test_mixed_version_reads(started_cluster, catalog_table):
    """Parallel replicas and `*Cluster` functions over an old and a new replica return the single-node result."""
    namespace, table, location = catalog_table
    database = f"cat_mixed_{uuid.uuid4().hex[:8]}"
    # The replicas receive `SELECT ... FROM <database>.<table>`, so every node needs the database under the same name,
    # as on a Cloud service, where the catalog database is shared.
    for node in (node_old, node_new):
        _create_catalog_database(node, database)

    catalog_table_sql = f"{database}.`{namespace}.{table}`"
    table_function = f"icebergS3('{MINIO_URL}/{location}/', '{minio_access_key}', '{minio_secret_key}')"
    cluster_function = f"icebergS3Cluster('mixed_replicas', '{MINIO_URL}/{location}/', '{minio_access_key}', '{minio_secret_key}')"

    expected = {query: node_new.query(query) for query in _queries(catalog_table_sql)}
    assert expected[_queries(catalog_table_sql)[2]] == _expected_sales_rows()

    for initiator in (node_new, node_old):
        for query in _queries(catalog_table_sql):
            assert initiator.query(query, settings=PARALLEL_REPLICAS) == expected[query], (initiator.name, query)
        for query, reference in zip(_queries(cluster_function), _queries(table_function)):
            assert initiator.query(query) == initiator.query(reference), (initiator.name, query)

    # A data file goes to one replica, which in `mixed_replicas` can be either. A cluster of only the other version's
    # replica makes it read every file and send the rows to the initiator, in both directions.
    for initiator, peer_cluster in ((node_new, "only_old"), (node_old, "only_new")):
        settings = {**PARALLEL_REPLICAS, "cluster_for_parallel_replicas": peer_cluster}
        peer_function = cluster_function.replace("'mixed_replicas'", f"'{peer_cluster}'")
        for query in _queries(catalog_table_sql):
            assert initiator.query(query, settings=settings) == expected[query], (initiator.name, peer_cluster, query)
        for query, reference in zip(_queries(peer_function), _queries(table_function)):
            assert initiator.query(query) == initiator.query(reference), (initiator.name, peer_cluster, query)

    for node in (node_old, node_new):
        node.query(f"DROP DATABASE {database}")


def test_mixed_version_insert_select_defaults(started_cluster):
    """A distributed `INSERT ... SELECT FROM s3Cluster` forwarded to an old and a new node applies the destination `DEFAULT`s.

    https://github.com/ClickHouse/ClickHouse/pull/127657: the forwarded query carried the source structure without
    the `DEFAULT` expressions, and the nodes wrote `0`, `''` or `1970-01-01` for the fields missing in the files.
    """
    suffix = uuid.uuid4().hex[:8]
    path = f"{MINIO_URL}/iceberg-data/defaults_{suffix}"
    node_new.query(
        f"INSERT INTO FUNCTION s3('{path}/a.jsonl', '{minio_access_key}', '{minio_secret_key}', 'JSONEachRow', 'id UInt32') VALUES (1), (3)"
    )
    node_new.query(
        f"INSERT INTO FUNCTION s3('{path}/b.jsonl', '{minio_access_key}', '{minio_secret_key}', 'JSONEachRow', 'id UInt32, d Date, c String') "
        "VALUES (2, '2001-02-03', 'x')"
    )
    columns = "id UInt32, d Date DEFAULT '2000-01-01', c String DEFAULT 'unknown', e UInt32 DEFAULT id * 10"
    for node in (node_old, node_new):
        node.query(f"CREATE TABLE local_{suffix} ({columns}) ENGINE = MergeTree ORDER BY id")
    # `mixed_shards` splits the files between the two nodes by path, so the old one may get only the file that has every
    # column. `only_old` makes it read both files, including the one with missing columns.
    destinations = ("mixed_shards", "only_old")
    for destination in destinations:
        node_new.query(
            f"CREATE TABLE dist_{destination}_{suffix} AS local_{suffix} "
            f"ENGINE = Distributed('{destination}', default, local_{suffix}, rand())"
        )

    expected = "1\t2000-01-01\tunknown\t10\n2\t2001-02-03\tx\t20\n3\t2000-01-01\tunknown\t30\n"
    files = f"'{path}/*.jsonl', '{minio_access_key}', '{minio_secret_key}', 'JSONEachRow'"
    # The initiator is the new server, which forwards the `INSERT ... SELECT` to the old one. An old initiator is not
    # checked: 26.6 inserts the converted `s3` once per replica, which
    # https://github.com/ClickHouse/ClickHouse/pull/120488 fixed later, independently of the version of the replicas.
    for destination in destinations:
        for source, settings in (
            (f"s3Cluster('mixed_replicas', {files})", {}),
            (f"s3({files})", PARALLEL_REPLICAS),
        ):
            node_new.query(
                f"INSERT INTO dist_{destination}_{suffix} SELECT * FROM {source}",
                settings={"parallel_distributed_insert_select": 2, **settings},
            )
            assert node_new.query(f"SELECT * FROM dist_{destination}_{suffix} ORDER BY id") == expected, (destination, source)
            for node in (node_old, node_new):
                node.query(f"TRUNCATE TABLE local_{suffix}")

    for destination in destinations:
        node_new.query(f"DROP TABLE dist_{destination}_{suffix}")
    for node in (node_old, node_new):
        node.query(f"DROP TABLE local_{suffix}")
