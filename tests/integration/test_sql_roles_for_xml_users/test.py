import pytest
import os

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/access_control_path.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_user_grants():
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/users.xml"),
        "/etc/clickhouse-server/users.d/users.xml",
    )
    instance.query("system reload users")
    instance.wait_for_log_line("performing update on configuration")
    assert instance.query("show grants for user1") == ""
    
    instance.query("create role if not exists role1")
    instance.replace_in_config("/etc/clickhouse-server/users.d/users.xml", "<grants></grants>", "<grants><query>GRANT role1</query></grants>")
    instance.query("system reload users")
    instance.wait_for_log_line("performing update on configuration")
    assert instance.query("show grants for user1") == "GRANT role1 TO user1\n"

    # Make sure that assigning roles created in SQL to XML users works after restart
    instance.restart_clickhouse()
    assert instance.query("show grants for user1") == "GRANT role1 TO user1\n"

    # Removing the role via SQL should be handled gracefully with a warning in the log
    instance.query("drop role role1")
    instance.query("system reload users")
    instance.wait_for_log_line("performing update on configuration")
    instance.wait_for_log_line("Role role1 is not defined and will be ignored for grant query 'GRANT role1'.")
    assert instance.query("show grants for user1") == "GRANT NONE TO user1\n"
