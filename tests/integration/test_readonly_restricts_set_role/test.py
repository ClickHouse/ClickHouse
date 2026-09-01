import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `readonly_restricts_set_role` is absent from `helpers/0_common_instance_config.xml`, so an instance with
# no `main_configs` runs at the compatibility default `false`.
node_on = cluster.add_instance(
    "node_on", main_configs=["configs/readonly_restricts_set_role.xml"]
)
node_off = cluster.add_instance("node_off")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        for node in (node_on, node_off):
            node.query(
                "CREATE ROLE ro_role, other_role;"
                "CREATE USER probe IDENTIFIED WITH plaintext_password BY 'x';"
                "GRANT ro_role, other_role TO probe;"
                "ALTER USER probe DEFAULT ROLE ro_role;"
            )

        yield cluster

    finally:
        cluster.shutdown()


def as_probe(node, sql, readonly):
    return node.query_and_get_answer_with_error(
        sql, user="probe", password="x", settings={"readonly": readonly}
    )


@pytest.mark.parametrize(
    "readonly, statement",
    [
        (1, "SET ROLE other_role"),
        # The other `ASTSetRoleQuery::Kind` reaching the check.
        (1, "SET ROLE DEFAULT"),
        # `readonly = 2` is what the HTTP execution path sets for safe methods.
        (2, "SET ROLE other_role"),
    ],
)
def test_set_role_refused_when_enabled(readonly, statement):
    _, error = as_probe(node_on, statement, readonly)
    assert "READONLY" in error


def test_set_role_allowed_without_readonly():
    answer, error = as_probe(node_on, "SET ROLE other_role; SELECT currentRoles()", 0)
    assert error == ""
    assert answer.strip() == "['other_role']"


def test_set_role_allowed_at_default_settings():
    answer, error = as_probe(node_off, "SET ROLE other_role; SELECT currentRoles()", 1)
    assert error == ""
    assert answer.strip() == "['other_role']"
