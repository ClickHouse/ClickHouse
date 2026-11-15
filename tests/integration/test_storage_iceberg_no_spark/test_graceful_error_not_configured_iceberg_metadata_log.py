import pytest
import os

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_last_snapshot,
    get_uuid_str
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_graceful_not_configured_iceberg_metadata_log(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node2"]
    TABLE_NAME = "test_graceful_not_configured_iceberg_metadata_log_" + get_uuid_str()

    create_iceberg_table("s3", instance, TABLE_NAME, started_cluster_iceberg_no_spark, "(c0 Int)")
    error_message = instance.query_and_get_error(f"SELECT 1 FROM {TABLE_NAME}", settings={"iceberg_metadata_log_level": 'metadata'})

    assert "BAD_ARGUMENTS" in error_message, f"Unexpected error message: {error_message}"
