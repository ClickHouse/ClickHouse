# coding: utf-8

import os
import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

DOCKER_COMPOSE_PATH = get_docker_compose_path()
BEARER_PORT = 8001
DLF_PORT = 8002

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=[])

cluster.base_cmd.extend(
    ["--file", os.path.join(DOCKER_COMPOSE_PATH, "docker_compose_paimon_rest_catalog.yml")]
)


def wait_for_healthy(cluster, service_name, timeout=60):
    docker_id = cluster.get_instance_docker_id(service_name)
    container = cluster.get_docker_handle(docker_id)
    start = time.time()
    while time.time() - start < timeout:
        info = container.client.api.inspect_container(container.name)
        if info["State"]["Health"]["Status"] == "healthy":
            return
        time.sleep(1)
    raise Exception(f"Container {service_name} did not become healthy in {timeout}s")


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        wait_for_healthy(cluster, "paimon-rest-bearer")
        wait_for_healthy(cluster, "paimon-rest-dlf")
        yield cluster
    finally:
        cluster.shutdown()


def test_paimon_rest_catalog(started_cluster):
    bearer_ip = cluster.get_instance_ip("paimon-rest-bearer")
    bearer_container_id = cluster.get_instance_docker_id("paimon-rest-bearer")

    # clean warehouse data path
    run_and_check(
        [f'docker exec {bearer_container_id} bash -c "rm -rf /var/lib/clickhouse/user_files/warehouse/*"'],
        shell=True,
    )

    node.query("DROP DATABASE IF EXISTS paimon_rest_db SYNC;")
    node.query(
        f"CREATE DATABASE paimon_rest_db ENGINE = DataLakeCatalog('http://{bearer_ip}:{BEARER_PORT}')"
        f" SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse',"
        f" catalog_credential='bearer-token-xxx-xxx-xxx';",
        settings={"allow_experimental_database_paimon_rest_catalog": 1},
    )

    # create database via REST API
    requests.post(
        f"http://{bearer_ip}:{BEARER_PORT}/v1/paimon/databases",
        json={"name": "test"},
        headers={"Authorization": "Bearer bearer-token-xxx-xxx-xxx"},
    )

    # create table via REST API
    requests.post(
        f"http://{bearer_ip}:{BEARER_PORT}/v1/paimon/databases/test/tables",
        json={
            "identifier": {"database": "test", "object": "test_table"},
            "schema": {
                "fields": [
                    {
                        "id": 0,
                        "name": "f_string",
                        "type": "string",
                        "description": "string",
                    },
                    {
                        "id": 1,
                        "name": "f_int",
                        "type": "int",
                        "description": "int",
                    },
                    {
                        "id": 2,
                        "name": "f_bigint",
                        "type": "bigint",
                        "description": "bigint",
                    },
                ],
                "partitionKeys": ["f_string"],
                "primaryKeys": [],
                "options": {},
                "comment": "test table",
            },
        },
        headers={"Authorization": "Bearer bearer-token-xxx-xxx-xxx"},
    )

    assert (
        node.query("SHOW TABLES;", database="paimon_rest_db")
        == "test.test_table\n"
    )
    assert node.query(
        "DESC `test.test_table`;", database="paimon_rest_db"
    ) == (
        "f_string\tNullable(String)\t\t\t\t\t\n"
        "f_int\tNullable(Int32)\t\t\t\t\t\n"
        "f_bigint\tNullable(Int64)\t\t\t\t\t\n"
    )
    assert (
        node.query(
            "SELECT count(1) FROM `test.test_table`;", database="paimon_rest_db"
        )
        == "0\n"
    )

    # insert data via the paimon container
    insert_cmd = 'java -jar /opt/paimon/paimon-server.jar "insert" "file:///var/lib/clickhouse/user_files/warehouse/" "test" "test_table"'
    run_and_check(
        [f"docker exec {bearer_container_id} bash -c '{insert_cmd}'"],
        shell=True,
    )

    assert (
        node.query(
            "SELECT count(1) FROM `test.test_table`;", database="paimon_rest_db"
        )
        == "10\n"
    )

    # Test DLF authentication
    dlf_ip = cluster.get_instance_ip("paimon-rest-dlf")
    node.query("DROP DATABASE IF EXISTS paimon_rest_db_dlf SYNC;")
    node.query(
        f"CREATE DATABASE paimon_rest_db_dlf ENGINE = DataLakeCatalog('http://{dlf_ip}:{DLF_PORT}')"
        f" SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse',"
        f" dlf_access_key_id='accessKeyId', dlf_access_key_secret='accessKeySecret',"
        f" region='cn-hangzhou';",
        settings={"allow_experimental_database_paimon_rest_catalog": 1},
    )
    node.query("SHOW TABLES;", database="paimon_rest_db_dlf")

    node.query("DROP DATABASE IF EXISTS paimon_rest_db_dlf SYNC;")
    node.query(
        f"CREATE DATABASE paimon_rest_db_dlf ENGINE = DataLakeCatalog('http://{dlf_ip}:{DLF_PORT}')"
        f" SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse',"
        f" dlf_access_key_id='accessKeyIdxx', dlf_access_key_secret='accessKeySecret',"
        f" region='cn-hangzhou';",
        settings={"allow_experimental_database_paimon_rest_catalog": 1},
    )
    assert "" == node.query("SHOW TABLES;", database="paimon_rest_db_dlf")
