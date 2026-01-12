import pytest
from helpers.cluster import ClickHouseCluster
from .common import *

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_token_with_codecs.xml"],
    user_configs=["configs/users_encrypted.xml"],
    with_hashicorp_vault=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.set_hashicorp_vault_startup_command(vault_startup_command_codecs)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_token_with_codecs_work(started_cluster):
    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="test_password"
        )
        == "test_user\n"
    )
