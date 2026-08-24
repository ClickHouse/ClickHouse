import logging
from datetime import datetime

import pytest

from helpers.cluster import ClickHouseCluster

log_dir = "/var/log/clickhouse-server/"
cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    cluster.add_instance(
        "file-names-from-config",
        main_configs=["configs/config-file-template.xml"],
        clickhouse_log_file=None,
        clickhouse_error_log_file=None,
    )
    cluster.add_instance(
        "file-names-from-params",
        clickhouse_log_file=log_dir + "clickhouse-server-%Y-%m.log",
        clickhouse_error_log_file=log_dir + "clickhouse-server-%Y-%m.err.log",
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_check_file_names(started_cluster):
    now = datetime.now()
    log_file = log_dir + f"clickhouse-server-{now.strftime('%Y-%m')}.log"
    err_log_file = log_dir + f"clickhouse-server-{now.strftime('%Y-%m')}.err.log"
    logging.debug(f"log_file {log_file} err_log_file {err_log_file}")

    for name, instance in started_cluster.instances.items():
        files = instance.exec_in_container(
            ["bash", "-c", f"ls -lh {log_dir}"], nothrow=True
        )

        logging.debug(f"check instance '{name}': {log_dir} contains: {files}")

        assert (
            instance.exec_in_container(["bash", "-c", f"ls {log_file}"], nothrow=True)
            == log_file + "\n"
        )

        assert (
            instance.exec_in_container(
                ["bash", "-c", f"ls {err_log_file}"], nothrow=True
            )
            == err_log_file + "\n"
        )
