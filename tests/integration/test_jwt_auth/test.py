import os
import pytest
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/verify_static_key.xml"],
    user_configs=["configs/users.xml"],
)
client = cluster.add_instance(
    "client",
    main_configs=["configs/verify_static_key.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_static_key(started_cluster):
    res = client.exec_in_container(
        [
            "bash",
            "-c",
            f'curl -H "X-ClickHouse-JWT-Token: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJqd3RfdXNlciIsInJlc291cmNlX2FjY2VzcyI6InZpZXctcHJvZmlsZSJ9.TVnAmEMZeUqG-BD2K4f3Hk6LRvCiTr28W9dbjSGzi0Q" "http://{cluster.get_instance_ip(instance.name)}:8123/?query=SELECT%20currentUser()"',
        ]
    )
    assert res == "jwt_user\n"


def test_static_jwks(started_cluster):
    res = client.exec_in_container(
        [
            "bash",
            "-c",
            f'curl -H "X-ClickHouse-JWT-Token: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Im15a2lkIiwidHlwIjoiSldUIn0.eyJzdWIiOiJqd3RfdXNlciIsInJlc291cmNlX2FjY2VzcyI6InZpZXctcHJvZmlsZSIsImlzcyI6InRlc3RfaXNzIn0.RnSXgwJBQGf9ph-HR0-tgN2ky87vvLtPZzOwSRxGwAQjLBIBSJ9t4JXB_DVK9MpVq7fV7c7_MtohX_fwqT9KhkmrRluyRvPBfImFkxmRJZMdNwQpr3RLm2Lj7NlINFIoAUnkMiNBN7SK0JA0F3ppi4CVZLahDsvWm4ddghoiAQwpqpjQIsDK3O2EWUxWRwhp28mY4auPcbutTD82Kzv1x_Zjygo1yEMqAid_4bfawkeAAERFUel3GCLrAqTNkFbXX_uWkd0bSOvPR9zzyrou0w2O0dborEHPwIMdxtJzfpymLrR_nvzH9xnt8ohhVvNojAekTnOTQrg3xQHUXAJJQA" "http://{cluster.get_instance_ip(instance.name)}:8123/?query=SELECT%20currentUser()"',
        ]
    )
    assert res == "jwt_user\n"
