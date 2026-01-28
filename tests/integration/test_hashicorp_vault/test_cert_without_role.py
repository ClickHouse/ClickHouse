import pytest
from helpers.cluster import ClickHouseCluster
from .common import *

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/config_cert_without_role.xml",
    ],
    user_configs=["configs/users.xml"],
    with_hashicorp_vault=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.set_hashicorp_vault_startup_command(vault_startup_command_cert)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cert_without_role_config_with_hashicorp_vault(started_cluster):
    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )
