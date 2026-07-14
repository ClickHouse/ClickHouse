import pytest
from helpers.cluster import ClickHouseCluster
from .common import vault_startup_command_xml_special

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_token.xml"],
    user_configs=["configs/users_xml_special.xml"],
    with_hashicorp_vault=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.set_hashicorp_vault_startup_command(vault_startup_command_xml_special)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_xml_special_chars(started_cluster):
    assert (
        instance.query(
            "SELECT currentUser()", user="test_user", password="pass&word>"
        )
        == "test_user\n"
    )
