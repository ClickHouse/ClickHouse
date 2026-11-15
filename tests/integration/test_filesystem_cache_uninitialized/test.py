import os
import pytest

from helpers.cluster import ClickHouseCluster


def test_uninitialized_error():
    cluster = ClickHouseCluster(__file__)

    try:
        cluster.add_instance(
            "node",
            main_configs=[
                "config.d/storage.xml",
            ],
            with_minio=True,
        )

        with pytest.raises(Exception) as exc_info:
            cluster.start()

            assert (
                "Code: 471. DB::Exception: Cache disk 'disk1' is used as a temporary storage, load_metadata_asynchronously is disallowed. (INVALID_SETTING_VALUE)"
                in str(exc_info.value)
            )

        # Ensure that asynchronous metadata loading is disabled when cache is used as temporary data storage.
        error_logs_file = os.path.join(
            cluster.instances_dir,
            "node",
            "logs",
            "clickhouse-server.err.log",
        )
        with open(error_logs_file, "r") as f:
            logs = f.read()
            assert (
                "Code: 471. DB::Exception: Cache disk 'disk1' is used as a temporary storage, load_metadata_asynchronously is disallowed. (INVALID_SETTING_VALUE)"
                in logs
            )

    finally:
        cluster.shutdown()
