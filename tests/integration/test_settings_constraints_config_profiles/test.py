import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/access_control.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

OPERATOR = {"user": "operator-internal", "password": "operator_pwd"}
CONSOLE = {"user": "sql-console", "password": "console_pwd"}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_config_actor_can_create_sql_user_referencing_config_profile(start_cluster):
    # `operator-internal` is a config-defined admin. Via the `operator` profile it inherits the
    # `default` profile's CONST on `database_replicated_allow_explicit_uuid`. The config-defined
    # `sql-console` profile relaxes that constraint. A config-defined actor must be trusted to
    # create a user referencing such a profile (this mirrors how the cloud operator provisions
    # the `sql-console` user).
    node.query("DROP USER IF EXISTS `sql-console`", **OPERATOR)
    node.query(
        "CREATE USER `sql-console` IDENTIFIED WITH plaintext_password BY 'console_pwd' "
        "SETTINGS PROFILE `sql-console`",
        **OPERATOR,
    )

    # The resulting SQL user references only a config-defined profile, so it must be able to log in
    # and run queries even though that profile relaxes a constraint the `default` profile marks CONST.
    assert node.query("SELECT 1", **CONSOLE).strip() == "1"

    # The config profile's value override is in effect for the SQL user (default sets cloud_mode=1 CONST).
    assert (
        node.query(
            "SELECT value FROM system.settings WHERE name = 'cloud_mode'", **CONSOLE
        ).strip()
        == "0"
    )

    # The config profile relaxed database_replicated_allow_explicit_uuid (default marks it CONST=0),
    # so sql-console may set it to 1 — verify the relaxed constraint is actually in effect.
    assert (
        node.query(
            "SELECT value FROM system.settings WHERE name = 'database_replicated_allow_explicit_uuid'",
            settings={"database_replicated_allow_explicit_uuid": 1},
            **CONSOLE,
        ).strip()
        == "1"
    )

    node.query("DROP USER IF EXISTS `sql-console`", **OPERATOR)


def test_sql_actor_cannot_escalate_past_const_constraint(start_cluster):
    # A SQL-defined user bound by the `default` profile's CONST must not be able to create another
    # user that escapes that constraint. This is the escalation path the enforcement closes.
    node.query("DROP USER IF EXISTS tenant, evil", **OPERATOR)
    node.query(
        "CREATE USER tenant IDENTIFIED WITH plaintext_password BY 'tenant_pwd'", **OPERATOR
    )
    node.query("GRANT CREATE USER ON *.* TO tenant", **OPERATOR)

    error = node.query_and_get_error(
        "CREATE USER evil SETTINGS database_replicated_allow_explicit_uuid = 1",
        user="tenant",
        password="tenant_pwd",
    )
    assert "should not be changed" in error

    node.query("DROP USER IF EXISTS tenant, evil", **OPERATOR)
