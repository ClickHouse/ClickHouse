# coding: utf-8

import os
from time import sleep
from typing import Literal

import pytest
import requests

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=[])
PORT = 8001
DLF_PORT = 8002

@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()

def start_bearer_token_server():
    instance_id = cluster.get_instance_docker_id("node")
    # copy paimon rest catalog to docker container    
    run_and_check(
        ["docker cp {local} {cont_id}:{dist}".format(
            local=os.path.join(SCRIPT_DIR, f"paimon-rest-catalog"),
            cont_id=instance_id,
            dist=f"/root/paimon-rest-catalog",
        )]
        ,
        shell=True
    )

    # build paimon rest catalog from chunks
    run_and_check(
        ["docker exec {cont_id} bash -lc \"cd /root/paimon-rest-catalog && cat chunk_* > paimon-server-starter-1.0-SNAPSHOT.jar\"".format(
            cont_id=instance_id,
        )]
        , 
        shell=True
    )

    # start paimon rest catalog
    start_cmd = f'''nohup java -jar paimon-server-starter-1.0-SNAPSHOT.jar "server" "file:///tmp/warehouse/" "bearer" "0.0.0.0" "{PORT}" &!'''
    run_and_check(
        ["docker exec {cont_id} bash -lc 'cd /root/paimon-rest-catalog && {start_cmd}'".format(
            cont_id=instance_id,
            start_cmd=start_cmd,
        )], shell=True, nothrow=True
    )

def start_dlf_token_server():
    # start dlf paimon rest catalog
    instance_id = cluster.get_instance_docker_id("node")
    start_cmd = f'''nohup java -jar paimon-server-starter-1.0-SNAPSHOT.jar "server" "file:///tmp/warehouse/" "dlf" "0.0.0.0" "{DLF_PORT}" &!'''
    run_and_check(
        ["docker exec {cont_id} bash -lc 'cd /root/paimon-rest-catalog && {start_cmd}'".format(
            cont_id=instance_id,
            start_cmd=start_cmd,
        )], shell=True, nothrow=True
    )

def test_paimon_rest_catalog(started_cluster):
    start_bearer_token_server()
    paimon_rest_catalog_container_ip = cluster.get_instance_ip("node")
    # clean warehouse data path
    instance_id = cluster.get_instance_docker_id("node")
    run_and_check(
        ["docker exec {cont_id} bash -lc \"rm -fr /tmp/warehouse\"".format(
            cont_id=instance_id,
        )]
        , 
        shell=True
    )

    node.query("DROP DATABASE IF EXISTS paimon_rest_db SYNC;")
    node.query(f"create database paimon_rest_db engine = DataLakeCatalog('http://{paimon_rest_catalog_container_ip}:{PORT}') SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse', catalog_credential='bearer-token-xxx-xxx-xxx';", settings={"allow_experimental_database_paimon_rest_catalog": 1})
    # create database
    requests.post(
        f"http://{paimon_rest_catalog_container_ip}:{PORT}/v1/paimon/databases",
        json={
            "name": "test",
        },
        headers={
            "Authorization": "Bearer bearer-token-xxx-xxx-xxx",
        }
    )
    # create table
    requests.post(
        f"http://{paimon_rest_catalog_container_ip}:{PORT}/v1/paimon/databases/test/tables",
        json=
        {
        "identifier": {
            "database": "test",
            "object": "test_table"
        },
        "schema": {
            "fields": [
            {
                "id": 0,
                "name": "f_string",
                "type": "string",
                "description": "string"
            },
            {
                "id": 1,
                "name": "f_int",
                "type": "int",
                "description": "int"
            },
            {
                "id": 2,
                "name": "f_bigint",
                "type": "bigint",
                "description": "bigint"
            }
            ],
            "partitionKeys": [
            "f_string"
            ],
            "primaryKeys": [],
            "options": {},
            "comment": "test table"
        }
        },
        headers={
            "Authorization": "Bearer bearer-token-xxx-xxx-xxx",
        }
    )

    assert(
        node.query("show tables;", database="paimon_rest_db") == "test.test_table\n"
    )
    assert(
        node.query("desc `test.test_table`;", database="paimon_rest_db") == "f_string\tNullable(String)\t\t\t\t\t\nf_int\tNullable(Int32)\t\t\t\t\t\nf_bigint\tNullable(Int64)\t\t\t\t\t\n"
    )
    assert(
        node.query("select count(1) from `test.test_table`;", database="paimon_rest_db") == "0\n"
    )

    # insert data
    insert_cmd = '''java -jar paimon-server-starter-1.0-SNAPSHOT.jar "insert" "file:///tmp/warehouse/" "test" "test_table"'''
    run_and_check(
        ["docker exec {cont_id} bash -lc \"cd /root/paimon-rest-catalog && {insert_cmd}\"".format(
            cont_id=instance_id,
            insert_cmd=insert_cmd,
        )]
        , 
        shell=True
    )

    assert(
        node.query("select count(1) from `test.test_table`;", database="paimon_rest_db") == "10\n"
    )

    start_dlf_token_server()
    node.query("DROP DATABASE IF EXISTS paimon_rest_db_dlf SYNC;")
    node.query(f"create database paimon_rest_db_dlf engine = DataLakeCatalog('http://{paimon_rest_catalog_container_ip}:{DLF_PORT}') SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse', dlf_access_key_id='accessKeyId', dlf_access_key_secret='accessKeySecret', region='cn-hangzhou';", settings={"allow_experimental_database_paimon_rest_catalog": 1})
    node.query("show tables;", database="paimon_rest_db_dlf")

    node.query("DROP DATABASE IF EXISTS paimon_rest_db_dlf SYNC;")
    node.query(f"create database paimon_rest_db_dlf engine = DataLakeCatalog('http://{paimon_rest_catalog_container_ip}:{DLF_PORT}') SETTINGS catalog_type='paimon_rest', warehouse='restWarehouse', dlf_access_key_id='accessKeyIdxx', dlf_access_key_secret='accessKeySecret', region='cn-hangzhou';", settings={"allow_experimental_database_paimon_rest_catalog": 1})
    assert "" == node.query("show tables;", database="paimon_rest_db_dlf")
