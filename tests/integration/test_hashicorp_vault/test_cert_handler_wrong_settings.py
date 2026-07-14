from helpers.cluster import ClickHouseCluster
from .common import vault_startup_command


def test_vault_reject_global_accept():
    cluster = ClickHouseCluster(__file__)
    instance = cluster.add_instance(
        "instance",
        main_configs=[
            "configs/config_https_no_ca.xml",
            "configs/config_global_accept.xml",
        ],
        user_configs=["configs/users.xml"],
        with_hashicorp_vault=True,
    )
    cluster.set_hashicorp_vault_startup_command(vault_startup_command)
    failed_to_start = False
    try:
        cluster.start()
    except Exception:
        failed_to_start = True
    assert failed_to_start
    message_found = instance.contains_in_log("SSL Exception", from_host=True)
    assert message_found
