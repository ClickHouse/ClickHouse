import pytest
from helpers.cluster import ClickHouseCluster

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
            f'curl -H "X-ClickHouse-JWT-Token: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJqd3RfdXNlciIsInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbInZpZXctcHJvZmlsZSJdfX19.nXe-V8b_LFpmEnwSy01HcbbNCBw8rTZyo5BZFO3PDXU" "http://{cluster.get_instance_ip(instance.name)}:8123/?query=SELECT%20currentUser()"',
        ]
    )
    assert res == "jwt_user\n"
