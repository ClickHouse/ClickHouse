import os.path as p
import random
import threading
import time
import pytest
import logging

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException

import json
import subprocess

import socket

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kerberos.xml"],
    user_configs=["configs/users.xml"],
    with_kerberos_kdc=True,
    clickhouse_path_dir="clickhouse_path",
)


# Fixtures


@pytest.fixture(scope="module")
def kerberos_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kerberos_setup_teardown():
    yield  # run test


# Tests


def test_kerberos_auth_with_keytab(kerberos_cluster):
    logging.debug("kerberos test")
    instance.exec_in_container(
        ["bash", "-c", "kinit -V -k -t /tmp/keytab/kuser.keytab kuser"]
    )
    assert (
        instance.exec_in_container(
            ["bash", "-c", "echo 'select currentUser()' | curl -vvv --negotiate -u : http://{}:8123/ --data-binary @-".format(instance.hostname)]
        )
        == "kuser\n"
    )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
