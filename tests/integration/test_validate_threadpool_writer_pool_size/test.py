import os
import pytest

from helpers.cluster import ClickHouseCluster


# Tests the validation of threadpool_writer_pool_size setting:
def test_invalid_setting():
    cluster = ClickHouseCluster(__file__)

    try:
        cluster.add_instance(
            "node_invalid_setting",
            main_configs=["configs/invalid_setting.xml"],
        )

        # The cluster should fail to start when trying to start the node with invalid settings
        with pytest.raises(Exception) as exc_info:
            cluster.start()

            assert (
                "Code: 36. DB::Exception: A setting's value has to be greater than 0: while parsing setting 'threadpool_writer_pool_size' value. (BAD_ARGUMENTS)"
                in str(exc_info.value)
            )

        # Also check that the error logs contain the expected message
        logs = ""
        error_logs_file = os.path.join(
            cluster.instances_dir,
            "node_invalid_setting",
            "logs",
            "clickhouse-server.err.log",
        )
        with open(error_logs_file, "r") as f:
            logs = f.read()

        assert (
            "Code: 36. DB::Exception: A setting's value has to be greater than 0: while parsing setting 'threadpool_writer_pool_size' value. (BAD_ARGUMENTS)"
            in logs
        )
    finally:
        cluster.shutdown()
