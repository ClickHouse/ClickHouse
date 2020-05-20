import os.path as p
import random
import threading
import time
import pytest

from random import randrange
import pika
from sys import getdefaultencoding

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager

import json
import subprocess

from google.protobuf.internal.encoder import _VarintBytes

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir='configs',
                                main_configs=['configs/rabbitmq.xml','configs/log_conf.xml'],
                                with_rabbitmq=True)
rabbitmq_id = ''


# Helpers

def check_rabbitmq_is_available():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          rabbitmq_id,
                          'rabbitmqctl', 
                          'await_startup'),
                         stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def enable_consistent_hash_plugin():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          rabbitmq_id,
                          "rabbitmq-plugins", "enable", "rabbitmq_consistent_hash_exchange"),
                         stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def wait_rabbitmq_is_available(max_retries=50):
    retries = 0
    while True:
        if check_rabbitmq_is_available():
            break
        else:
            retries += 1
            if retries > max_retries:
                raise "RabbitMQ is not available"
            print("Waiting for RabbitMQ to start up")
            time.sleep(1)


def wait_rabbitmq_plugin_enabled(max_retries=50):
    retries = 0
    while True:
        if enable_consistent_hash_plugin():
            break
        else:
            retries += 1
            if retries > max_retries:
                raise "RabbitMQ plugin is not available"
            print("Waiting for plugin")
            time.sleep(1)


def rabbitmq_check_result(result, check=False, ref_file='test_rabbitmq_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


# Fixtures

@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        global rabbitmq_id
        cluster.start()
        rabbitmq_id = instance.cluster.rabbitmq_docker_id
        print("rabbitmq_id is {}".format(rabbitmq_id))
        instance.query('CREATE DATABASE test')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_setup_teardown():
    wait_rabbitmq_is_available()
    wait_rabbitmq_plugin_enabled()
    print("RabbitMQ is available - running test")
    yield  # run test
    instance.query('DROP TABLE IF EXISTS test.rabbitmq')


# Tests



if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()

