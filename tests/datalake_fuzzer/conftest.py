import logging
import os
import sys
import time

import pytest

# The fuzzer runs on the integration test harness (`helpers.cluster` and friends) but is not part of
# the integration test suite, so that harness is imported from `tests/integration`.
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "integration")
)

# Before importing pyspark: prevents a lot of log spam.
logging.getLogger("py4j").setLevel(logging.ERROR)

from helpers.cluster import ClickHouseCluster, run_and_check
from helpers.config_cluster import minio_access_key, minio_secret_key
from foreign_writer_fuzz import FuzzContext, Gates
from foreign_writer_fuzz.spark_session import build_spark
from helpers.s3_tools import S3Uploader, prepare_s3_bucket

REST_CATALOG_DATABASE = "fwf_rest"


@pytest.fixture(autouse=True, scope="session")
def tune_local_port_range():
    # As `tests/integration/conftest.py`, whose fixtures do not apply outside the integration suite:
    # MinIO and the catalog use non-privileged ports, and the default range leads to EADDRNOTAVAIL.
    try:
        run_and_check(["sysctl net.ipv4.ip_local_port_range='55000 65535'"], shell=True)
    except Exception as ex:
        logging.warning(
            "Failed to run sysctl, the fuzzer may fail with EADDRINUSE: %s", ex
        )


def _load_rest_catalog(cluster, deadline):
    from pyiceberg.catalog import load_catalog

    last_error = None
    while time.monotonic() < deadline:
        try:
            catalog = load_catalog(
                "fwf",
                **{
                    "uri": f"http://localhost:{cluster.iceberg_rest_catalog_port}",
                    "type": "rest",
                    "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
                    "s3.access-key-id": minio_access_key,
                    "s3.secret-access-key": minio_secret_key,
                },
            )
            catalog.list_namespaces()
            return catalog
        except Exception as e:
            # The catalog container accepts connections before it serves requests.
            last_error = e
            time.sleep(1)
    raise TimeoutError(f"The Iceberg REST catalog did not come up: {last_error}")


@pytest.fixture(scope="module")
def fuzz_context():
    cluster = ClickHouseCluster(__file__, with_spark=True)
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            with_iceberg_catalog=True,
            stay_alive=True,
        )
        cluster.start()
        prepare_s3_bucket(cluster)
        # `helpers.paimon_utils.upload_paimon_table` uploads through this attribute.
        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        node = cluster.instances["node1"]
        cluster.spark_session = build_spark(cluster)
        ctx = FuzzContext(
            cluster=cluster, node=node, spark=cluster.spark_session, gates=Gates()
        )
        ctx.load_known_settings()
        ctx.rest_catalog = _load_rest_catalog(cluster, time.monotonic() + 120)
        ctx.query(
            f"CREATE DATABASE {REST_CATALOG_DATABASE} ENGINE = DataLakeCatalog('http://rest:8181/v1', {ctx.s3_credentials_sql()}) "
            "SETTINGS catalog_type = 'rest', warehouse = 'demo', storage_endpoint = 'http://minio1:9001/warehouse-rest'"
        )
        ctx.rest_catalog_database = REST_CATALOG_DATABASE
        logging.info("Foreign writer fuzz: %s", ctx.gates.describe())
        yield ctx
    finally:
        # The session carries this module's catalogs and extensions; stop it so that a later
        # module on the same pytest worker does not get it back from `getOrCreate`.
        try:
            spark = getattr(cluster, "spark_session", None)
            if spark is not None:
                spark.stop()
        finally:
            cluster.shutdown()
