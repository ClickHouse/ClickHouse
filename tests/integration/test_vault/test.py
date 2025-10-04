import pytest
from helpers.cluster import ClickHouseCluster
from .common import *

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_vault=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.set_vault_startup_command(vault_startup_command)
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_config_with_from_vault(started_cluster):
    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )
    assert True
