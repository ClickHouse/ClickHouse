import logging
import os
import threading
import pytest
import time
import string
from helpers.cluster import ClickHouseCluster, get_instances_dir


class SafeThread(threading.Thread):
    def __init__(self, target):
        super().__init__()
        self.target = target
        self.exception = None
    def run(self):
        try:
            self.target()
        except Exception as e: # pylint: disable=broad-except
            self.exception = e
    def join(self, timeout=None):
        super().join(timeout)
        if self.exception:
            raise self.exception


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, './{}/node/configs/config.d/storage_conf.xml'.format(get_instances_dir()))


def run_blob_storage_mocks(cluster):
    logging.info("Starting Blob Storage mocks")
    mocks = (
        ("unstable_proxy.py", "resolver", "8081"),
    )
    for mock_filename, container, port in mocks:
        container_id = cluster.get_container_id(container)
        current_dir = os.path.dirname(__file__)
        cluster.copy_file_to_container(container_id, os.path.join(current_dir, "blob_storage_mocks", mock_filename), mock_filename)
        cluster.exec_in_container(container_id, ["python", mock_filename, port], detach=True)

    # Wait for Blob Storage mocks to start
    for mock_filename, container, port in mocks:
        num_attempts = 100
        for attempt in range(num_attempts):
            ping_response = cluster.exec_in_container(cluster.get_container_id(container),
                                                              ["curl", "-s", f"http://localhost:{port}/"], nothrow=True)
            if ping_response != "OK":
                if attempt == num_attempts - 1:
                    assert ping_response == "OK", f'Expected "OK", but got "{ping_response}"'
                else:
                    time.sleep(1)
            else:
                logging.debug(f"mock {mock_filename} ({port}) answered {ping_response} on attempt {attempt}")
                break

    logging.info("Blob Storage mocks started")


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node",
                            main_configs=[
                                "configs/config.d/storage_conf.xml",
                                "configs/config.d/bg_processing_pool_conf.xml"],
                            with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        run_blob_storage_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "p_blob_storage",
        "index_granularity": 512
    }
    settings.update(additional_settings)

    create_table_statement = f"""
        CREATE TABLE {table_name} (
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}"""

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(create_table_statement)


def test_simple(cluster):
    node = cluster.instances["node"]
    create_table(node, "blob_storage_test")
    minio = cluster.minio_client
    pass