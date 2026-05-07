# Proof test for PR #102115: cluster mode with cluster_table_function_split_granularity=BUCKET
# poisons the QCC by writing 'no match' for row groups assigned to *other* workers.
# Root cause: src/Storages/ObjectStorage/StorageObjectStorageSource.cpp:524.
# Bug signature: result_2 < result_1 for an idempotent SELECT count() ... WHERE.

import logging, time, uuid as uuid_lib
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

BASE_URL = "http://rest:8181/v1"
CATALOG_NAME = "demo"


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance("node1", main_configs=["configs/cluster.xml"],
                             stay_alive=True, with_iceberg_catalog=True)
        cluster.add_instance("node2", main_configs=["configs/cluster.xml"],
                             stay_alive=True, with_iceberg_catalog=True)
        cluster.start()
        time.sleep(10)  # iceberg-rest container needs a moment after start
        yield cluster
    finally:
        cluster.shutdown()


def _load_catalog(started_cluster):
    return load_catalog(CATALOG_NAME, **{
        "uri": f"http://localhost:{started_cluster.iceberg_rest_catalog_port}",
        "type": "rest",
        "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
        "s3.access-key-id": minio_access_key,
        "s3.secret-access-key": minio_secret_key,
    })


def _create_iceberg_database(node):
    node.query(
        f"DROP DATABASE IF EXISTS {CATALOG_NAME}; "
        f"CREATE DATABASE {CATALOG_NAME} ENGINE = DataLakeCatalog("
        f"  '{BASE_URL}', 'minio', '{minio_secret_key}') "
        f"SETTINGS catalog_type='rest', warehouse='demo', "
        f"  storage_endpoint='http://minio:9000/warehouse-rest'",
        settings={"allow_database_iceberg": 1, "write_full_path_in_iceberg_metadata": 1},
    )


def test_qcc_bucket_split_does_not_poison_other_workers_buckets(started_cluster):
    n1 = started_cluster.instances["node1"]
    n2 = started_cluster.instances["node2"]
    namespace = f"qcc_bucket_{uuid_lib.uuid4().hex[:8]}"
    table_name = "t"

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="flag", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="payload", field_type=StringType(), required=False),
    )
    catalog = _load_catalog(started_cluster)
    catalog.create_namespace(namespace)

    # Force tiny row groups so a single parquet file has many row groups.
    # pyiceberg uses `write.parquet.row-group-limit` as pyarrow's `row_group_size`
    # (see pyiceberg/io/pyarrow.py::write_file).
    rows_per_group, n_groups = 256, 16
    iceberg_table = catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location=f"s3://warehouse-rest/data/{namespace}/{table_name}",
        properties={
            "format-version": "2",
            "write.parquet.row-group-limit": str(rows_per_group),
            "write.parquet.row-group-size-bytes": "4096",
            "write.target-file-size-bytes": str(1024 * 1024 * 1024),
        },
    )

    # One matching row per row group, evenly spread → both workers get matches.
    total_rows = rows_per_group * n_groups
    flags = [1 if (i % rows_per_group == 0) else 0 for i in range(total_rows)]
    df = pa.table({
        "id": list(range(total_rows)),
        "flag": flags,
        "payload": ["x" * 8] * total_rows,
    })
    expected_matches = sum(flags)
    assert expected_matches == n_groups
    iceberg_table.append(df)

    # Sanity: confirm the parquet file actually has multiple row groups,
    # otherwise the BUCKET splitter produces a single bucket and the bug
    # cannot manifest (would result in a silent no-op test).
    iceberg_table.refresh()
    data_files = list(iceberg_table.scan().plan_files())
    assert len(data_files) >= 1, "iceberg table has no data files"
    max_row_groups = 0
    for ft in data_files[:4]:
        with iceberg_table.io.new_input(ft.file.file_path).open() as f:
            max_row_groups = max(max_row_groups, pq.ParquetFile(f).num_row_groups)
    assert max_row_groups >= 4, (
        f"need >=4 row groups in a single parquet file but pyiceberg wrote "
        f"{max_row_groups}; QCC bucket-split bug cannot be exercised"
    )

    for node in (n1, n2):
        _create_iceberg_database(node)
        node.query("SYSTEM DROP QUERY CONDITION CACHE")

    settings = {
        "use_query_condition_cache": 1,
        "cluster_table_function_split_granularity": "bucket",
        "cluster_for_parallel_replicas": "cluster_simple",
        "parallel_replicas_for_cluster_engines": 1,
        "enable_parallel_replicas": 2,
        "allow_experimental_analyzer": 1,
    }
    q = f"SELECT count() FROM {CATALOG_NAME}.`{namespace}.{table_name}` WHERE flag = 1"

    # First run populates the cache; under the bug, also poisons it
    # (each worker writes 'no match' for the row groups it never read).
    r1 = int(n1.query(q, settings=settings).strip())
    # Second run consumes the cache.  Under the bug, the initiator reads
    # the poisoned matching_marks and skips most/all row groups → r2 < r1.
    r2 = int(n1.query(q, settings=settings).strip())

    logging.info("QCC bucket-split test: expected=%d  r1=%d  r2=%d",
                 expected_matches, r1, r2)
    # Setup gate: first run must return all matches.
    assert r1 == expected_matches, f"first run returned {r1}, expected {expected_matches}"
    # Bug gate: second run must equal first run.  Under the bug, r2 < r1
    # because each worker poisoned the QCC for the *other* worker's buckets.
    assert r1 == r2, (
        f"QCC bucket-split poisoning detected: r1={r1}, r2={r2}. "
        "Workers wrote 'no match' for row groups they never read."
    )
