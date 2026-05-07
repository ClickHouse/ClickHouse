"""
Proof-of-concept test for suspected bug in PR #102115:

  Cluster mode with `cluster_table_function_split_granularity=BUCKET`
  poisons the query condition cache by writing 'no match' for row groups
  assigned to *other* workers.

Root cause (StorageObjectStorageSource.cpp:524):

    size_t total_groups = buckets_opt->second;          // = full file row-group count
    ...
    for (size_t i = 0; i < total_groups; ++i)
        if (!matched_set.contains(i))
            unmatched_ranges.push_back({i, i + 1});      // (1) BUG: includes
                                                         //     row groups that
                                                         //     were never read
                                                         //     by *this* worker

    query_condition_cache->write(... unmatched_ranges, total_groups, false);

`getMatchedBuckets()` only reports row groups that this worker actually
processed (`row_group.need_to_process`). With BUCKET split granularity each
worker processes a strict subset of the file's row groups; the worker then
writes "all the *other* workers' row groups didn't match the predicate" into
the query condition cache.

Test strategy:
  1. Create an iceberg table via DataLakeCatalog (REST catalog) so the
     storage_id has a stable UUID — required for the cache write to be
     persisted (`QueryConditionCache::write` early-returns on UUIDHelpers::Nil).
  2. Append a single parquet file with many row groups and a selective
     predicate that matches rows in *every* row group.
  3. Run the query with `cluster_table_function_split_granularity=bucket`
     across two replicas, with `use_query_condition_cache=1`.
     Each worker writes (poisoned) "no match" entries for the buckets it
     didn't own.
  4. Run the *same* query a second time. With the bug, the QCC says "no
     row groups match" -> the file is entirely skipped -> result is 0
     instead of the true count.

Bug signature: result_2 is strictly smaller than result_1.
"""

import logging
import time
import uuid as uuid_lib

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
        cluster.add_instance(
            "node1",
            main_configs=["configs/cluster.xml"],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/cluster.xml"],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
        )
        cluster.start()
        # The REST catalog container needs a moment after cluster start.
        time.sleep(10)
        yield cluster
    finally:
        cluster.shutdown()


def _load_catalog(started_cluster):
    base_url = f"http://localhost:{started_cluster.iceberg_rest_catalog_port}"
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": base_url,
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def _create_iceberg_database(node):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio:9000/warehouse-rest",
    }
    node.query(
        f"""
DROP DATABASE IF EXISTS {CATALOG_NAME};
CREATE DATABASE {CATALOG_NAME} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join(k + "=" + repr(v) for k, v in settings.items())}
        """,
        settings={
            "allow_database_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )


def test_qcc_bucket_split_does_not_poison_other_workers_buckets(started_cluster):
    n1 = started_cluster.instances["node1"]
    n2 = started_cluster.instances["node2"]

    test_id = uuid_lib.uuid4().hex[:8]
    namespace = f"qcc_bucket_{test_id}"
    table_name = "t"

    # iceberg-rest "demo" warehouse maps to s3://warehouse-rest/data/
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="flag", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="payload", field_type=StringType(), required=False),
    )

    catalog = _load_catalog(started_cluster)
    catalog.create_namespace(namespace)
    # Force tiny row groups so a single parquet file has many row groups
    # (>= number of replicas, so each worker is guaranteed at least one
    # bucket).  pyiceberg uses `write.parquet.row-group-limit` as the
    # `row_group_size` (in rows) it passes to pyarrow's ParquetWriter —
    # see pyiceberg/io/pyarrow.py::write_file.
    rows_per_group = 256
    n_groups = 16
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

    # Construct n_groups row groups with one matching row per row group,
    # evenly spread so both buckets the splitter assigns to workers MUST
    # contain matches.
    total_rows = rows_per_group * n_groups
    ids = list(range(total_rows))
    flags = [1 if (i % rows_per_group == 0) else 0 for i in range(total_rows)]
    payload = ["x" * 8] * total_rows
    df = pa.table({"id": ids, "flag": flags, "payload": payload})

    expected_matches = sum(flags)
    assert expected_matches == n_groups, expected_matches

    iceberg_table.append(df)

    # Verify the parquet file actually has multiple row groups.  If pyiceberg
    # ever changes the property semantics this test would silently degrade
    # into a no-op; the explicit check ensures CI surfaces that.
    iceberg_table.refresh()
    data_files = list(iceberg_table.scan().plan_files())
    assert len(data_files) >= 1, "iceberg table has no data files"
    file_io = iceberg_table.io
    max_row_groups = 0
    for ft in data_files[:4]:
        with file_io.new_input(ft.file.file_path).open() as f:
            pf = pq.ParquetFile(f)
            max_row_groups = max(max_row_groups, pf.num_row_groups)
    assert max_row_groups >= 4, (
        f"expected >=4 row groups in a single parquet file but pyiceberg "
        f"wrote {max_row_groups} (with row-group-limit={rows_per_group}, "
        f"{total_rows} rows).  Without multi-row-group files the QCC bucket "
        "split bug cannot be exercised — fix the parquet writer setup."
    )
    logging.info(
        "parquet file has %d row groups (need >1 to exercise the bug)",
        max_row_groups,
    )

    for node in (n1, n2):
        _create_iceberg_database(node)

    # Wipe any QCC state from earlier tests in the module.
    for node in (n1, n2):
        node.query("SYSTEM DROP QUERY CONDITION CACHE")

    settings = {
        "use_query_condition_cache": 1,
        "cluster_table_function_split_granularity": "bucket",
        "cluster_for_parallel_replicas": "cluster_simple",
        "parallel_replicas_for_cluster_engines": 1,
        "enable_parallel_replicas": 2,
        "allow_experimental_analyzer": 1,
    }

    q = (
        f"SELECT count() FROM {CATALOG_NAME}.`{namespace}.{table_name}` "
        f"WHERE flag = 1"
    )

    # First run populates the cache; under the bug, also poisons it.
    r1 = int(n1.query(q, settings=settings).strip())

    # Second run consumes the cache.  Under the bug, the initiator's
    # ObjectIteratorSplitByBuckets reads the (poisoned) matching_marks
    # for the file, sees that "no row groups match" and skips the file,
    # yielding 0 instead of `expected_matches`.
    r2 = int(n1.query(q, settings=settings).strip())

    logging.info(
        "QCC bucket-split test: expected=%d  first_run=%d  second_run=%d",
        expected_matches,
        r1,
        r2,
    )

    # First-run correctness gate (independent of the QCC bug).  If this
    # fails, something else is wrong; surface it explicitly.
    assert r1 == expected_matches, (
        f"first run returned {r1}, expected {expected_matches}; "
        "BUCKET split or iceberg setup is broken"
    )

    # Cache-poisoning gate.  This is the bug the test is designed to expose:
    # under the bug, r2 < r1 because workers wrote 'no match' entries for
    # row groups they never read.
    assert r1 == r2, (
        f"QCC bucket-split poisoning detected: first run returned {r1}, "
        f"second run returned {r2}.  The second run should hit the query "
        "condition cache and return the same count, but each worker poisoned "
        "the cache for the row groups assigned to the *other* worker."
    )
