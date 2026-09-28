import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/display_secrets.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def explain(user, show_secrets):
    return node.query(
        "EXPLAIN PLAN actions = 1 SELECT * FROM definer_view",
        user=user,
        settings={"format_display_secrets_in_show_and_select": show_secrets},
    )


def test_privilege_to_display_secrets_shows_the_plan(start_cluster):
    # The plan of a `SQL SECURITY DEFINER` view runs with the definer's privileges and holds the value
    # of the scalar subquery over a table the readers cannot access. With the server setting
    # `display_secrets_in_show_and_select` on, the `displaySecretsInShowAndSelect` privilege together
    # with the format setting shows the plan; either alone does not. The stateless test server keeps
    # the server setting off, so this combination is checked here.
    node.query(
        """
        CREATE USER definer;
        CREATE USER reader;
        CREATE USER auditor;
        GRANT displaySecretsInShowAndSelect ON *.* TO auditor;

        CREATE TABLE private_table (s String) ENGINE = Memory;
        INSERT INTO private_table VALUES ('PRIVATE_ROW_VALUE');
        GRANT SELECT ON private_table TO definer;
        GRANT CREATE TEMPORARY TABLE ON *.* TO definer;

        CREATE VIEW definer_view DEFINER = definer SQL SECURITY DEFINER AS
            SELECT number FROM numbers(1) WHERE toString(number) = (SELECT s FROM private_table)
            SETTINGS enable_analyzer = 1;
        GRANT SELECT ON definer_view TO definer, reader, auditor;
        """
    )

    for user, show_secrets in [("reader", 0), ("reader", 1), ("auditor", 0)]:
        plan = explain(user, show_secrets)
        assert "PRIVATE_ROW_VALUE" not in plan, (user, show_secrets, plan)
        assert "ReadFromSystemNumbers" not in plan, (user, show_secrets, plan)
        assert "plan hidden" in plan, (user, show_secrets, plan)

    plan = explain("auditor", 1)
    assert "PRIVATE_ROW_VALUE" in plan, plan
    assert "ReadFromSystemNumbers" in plan, plan
    assert "plan hidden" not in plan, plan

    # The definer sees it without the privilege.
    plan = explain("definer", 0)
    assert "PRIVATE_ROW_VALUE" in plan, plan
    assert "plan hidden" not in plan, plan
