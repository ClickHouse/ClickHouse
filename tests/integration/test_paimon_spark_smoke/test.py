"""In-tree consumer of the Paimon Spark bundle baked into the runner image
(ci/docker/integration/runner/Dockerfile): Spark in the runner writes a Paimon
table using only the baked jars — deliberately no `spark.jars.packages` — and
ClickHouse reads it back through `paimonLocal`. If the bundle disappears from
the image, the Paimon catalog fails to load here.
"""

import os
import re
import shutil

import pyspark
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import LocalUploader

# The warehouse is a host path shared by Spark and the uploader, so it is
# namespaced by xdist worker and harness run id (the pattern of
# helpers/iceberg_utils.iceberg_local_interop_dir): under the flaky check
# (--dist=each) several workers run this module at once, and two harness runs
# on one host must not share state either.
_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "master")
_RUN_ID = re.sub(r"[^A-Za-z0-9_]", "_", os.environ.get("INTEGRATION_TESTS_RUN_ID", ""))
WAREHOUSE = f"/var/lib/clickhouse/user_files/paimon_smoke_{_WORKER}" + (
    f"_{_RUN_ID}" if _RUN_ID else ""
)


def get_spark():
    return (
        pyspark.sql.SparkSession.builder.appName("paimon_smoke")
        # Deliberately no spark.jars.packages: the baked bundle must resolve.
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", f"file:{WAREHOUSE}")
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .master("local")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", stay_alive=True)
        cluster.start()
        # A rerun on the same worker inherits the same namespaced path, so any
        # stale warehouse from a previous run is removed before Spark starts.
        shutil.rmtree(WAREHOUSE, ignore_errors=True)
        # Registered on the cluster so shutdown() stops the session and the
        # JVM does not outlive this module.
        cluster.spark_session = get_spark()
        yield cluster
    finally:
        cluster.shutdown()


def test_spark_paimon_write_clickhouse_read(started_cluster):
    node = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.default")
    spark.sql(
        "CREATE TABLE paimon.default.smoke (id INT NOT NULL, val STRING) "
        "TBLPROPERTIES ('file.format' = 'parquet')"
    )
    spark.sql("INSERT INTO paimon.default.smoke VALUES (1, 'one'), (2, 'two')")

    table_dir = f"{WAREHOUSE}/default.db/smoke"
    LocalUploader(node).upload_directory(table_dir, table_dir)
    assert (
        node.query(f"SELECT id, val FROM paimonLocal('{table_dir}') ORDER BY id")
        == "1\tone\n2\ttwo\n"
    )
