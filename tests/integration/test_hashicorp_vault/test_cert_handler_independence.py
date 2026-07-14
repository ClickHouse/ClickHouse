import pytest
from helpers.cluster import ClickHouseCluster
from .common import vault_startup_command

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_https_no_ca_accept.xml"],
    user_configs=["configs/users.xml"],
    with_hashicorp_vault=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.set_hashicorp_vault_startup_command(vault_startup_command)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_vault_accept_global_reject(started_cluster):
    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )

    error = instance.query_and_get_error(
        "SELECT * FROM url('https://hashicorpvault:8220/v1/sys/health', 'JSONAsString', 'data String')"
    )
    assert error
