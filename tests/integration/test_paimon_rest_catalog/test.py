# coding: utf-8

import base64
import hashlib
import hmac
import json
import os
import time

import pytest
import requests

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

DOCKER_COMPOSE_PATH = get_docker_compose_path()
BEARER_PORT = 8001
DLF_PORT = 8002

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=[])

cluster.base_cmd.extend(
    [
        "--file",
        os.path.join(DOCKER_COMPOSE_PATH, "docker_compose_paimon_rest_catalog.yml"),
    ]
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
        [
            f'docker exec {bearer_container_id} bash -c "rm -rf /var/lib/clickhouse/user_files/warehouse/*"'
        ],
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

    assert node.query("SHOW TABLES;", database="paimon_rest_db") == "test.test_table\n"
    assert node.query("DESC `test.test_table`;", database="paimon_rest_db") == (
        "f_string\tNullable(String)\t\t\t\t\t\n"
        "f_int\tNullable(Int32)\t\t\t\t\t\n"
        "f_bigint\tNullable(Int64)\t\t\t\t\t\n"
    )
    assert (
        node.query("SELECT count(1) FROM `test.test_table`;", database="paimon_rest_db")
        == "0\n"
    )

    # insert data via the paimon container
    insert_cmd = 'java -jar /opt/paimon/paimon-server.jar "insert" "file:///var/lib/clickhouse/user_files/warehouse/" "test" "test_table"'
    run_and_check(
        [f"docker exec {bearer_container_id} bash -c '{insert_cmd}'"],
        shell=True,
    )

    assert (
        node.query("SELECT count(1) FROM `test.test_table`;", database="paimon_rest_db")
        == "10\n"
    )

    # Test DLF authentication
    dlf_ip = cluster.get_instance_ip("paimon-rest-dlf")
    dlf_container_id = cluster.get_instance_docker_id("paimon-rest-dlf")
    node.query("DROP DATABASE IF EXISTS paimon_rest_db_dlf SYNC;")
    node.query(
        f"CREATE DATABASE paimon_rest_db_dlf ENGINE = DataLakeCatalog('http://{dlf_ip}:{DLF_PORT}')"
        f" SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse',"
        f" dlf_access_key_id='accessKeyId', dlf_access_key_secret='accessKeySecret',"
        f" region='cn-hangzhou';",
        settings={"allow_experimental_database_paimon_rest_catalog": 1},
    )

    # The DLF server keeps its catalog in memory, separately from the bearer
    # server, so create a database and a table on it. The requests must be
    # signed the same way the server signs them (a Python port of Paimon's
    # `DLFAuthSignature`): the DLF v4 signature covers the concrete request
    # (method, resource path, query parameters and signed headers), so each
    # request gets its own signature.
    def dlf_request(method, resource_path, body=None):
        date_time = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        date = date_time[:8]
        region = "cn-hangzhou"
        scope = f"{date}/{region}/DlfNext/aliyun_v4_request"
        sign_headers = {
            "x-dlf-date": date_time,
            "x-dlf-content-sha256": "UNSIGNED-PAYLOAD",
            "x-dlf-version": "v1",
            "x-dlf-security-token": "",
        }
        if body:
            sign_headers["content-type"] = "application/json"
            sign_headers["content-md5"] = base64.b64encode(
                hashlib.md5(body.encode()).digest()
            ).decode()
        canonical_request = "\n".join(
            [method, resource_path, ""]
            + [f"{k}:{v}" for k, v in sorted(sign_headers.items())]
            + ["UNSIGNED-PAYLOAD"]
        )
        string_to_sign = "\n".join(
            [
                "DLF4-HMAC-SHA256",
                date_time,
                scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            ]
        )
        key = b"aliyun_v4accessKeySecret"
        for part in (date, region, "DlfNext", "aliyun_v4_request"):
            key = hmac.new(key, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
        headers = dict(sign_headers)
        headers["Authorization"] = (
            f"DLF4-HMAC-SHA256 Credential=accessKeyId/{scope},Signature={signature}"
        )
        response = requests.request(
            method,
            f"http://{dlf_ip}:{DLF_PORT}{resource_path}",
            data=body,
            headers=headers,
        )
        assert response.status_code == 200, (resource_path, response.status_code)

    dlf_request("POST", "/v1/paimon/databases", json.dumps({"name": "test_dlf"}))
    dlf_request(
        "POST",
        "/v1/paimon/databases/test_dlf/tables",
        json.dumps(
            {
                "identifier": {"database": "test_dlf", "object": "test_table"},
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
                    ],
                    "partitionKeys": ["f_string"],
                    "primaryKeys": [],
                    "options": {},
                    "comment": "test table",
                },
            }
        ),
    )

    # Every catalog request made by these queries is signed individually, so
    # they verify that signing works for requests other than the initial
    # config one made at CREATE DATABASE time. Before the per-request signing
    # fix the listing silently returned an empty result and the metadata
    # requests failed with HTTP 401.
    assert (
        node.query("SHOW TABLES;", database="paimon_rest_db_dlf")
        == "test_dlf.test_table\n"
    )
    assert node.query("DESC `test_dlf.test_table`;", database="paimon_rest_db_dlf") == (
        "f_string\tNullable(String)\t\t\t\t\t\n" "f_int\tNullable(Int32)\t\t\t\t\t\n"
    )
    assert (
        node.query(
            "SELECT count(1) FROM `test_dlf.test_table`;",
            database="paimon_rest_db_dlf",
        )
        == "0\n"
    )

    insert_cmd = 'java -jar /opt/paimon/paimon-server.jar "insert" "file:///var/lib/clickhouse/user_files/warehouse/" "test_dlf" "test_table"'
    run_and_check(
        [f"docker exec {dlf_container_id} bash -c '{insert_cmd}'"],
        shell=True,
    )

    assert (
        node.query(
            "SELECT count(1) FROM `test_dlf.test_table`;",
            database="paimon_rest_db_dlf",
        )
        == "10\n"
    )

    node.query("DROP DATABASE IF EXISTS paimon_rest_db_dlf SYNC;")
    with pytest.raises(QueryRuntimeException) as exc_info:
        node.query(
            f"CREATE DATABASE paimon_rest_db_dlf ENGINE = DataLakeCatalog('http://{dlf_ip}:{DLF_PORT}')"
            f" SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse',"
            f" dlf_access_key_id='accessKeyIdxx', dlf_access_key_secret='accessKeySecret',"
            f" region='cn-hangzhou';",
            settings={"allow_experimental_database_paimon_rest_catalog": 1},
        )
    message = str(exc_info.value)
    assert "Code: 86" in message, message
    assert "401" in message, message
