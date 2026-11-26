import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Every instance gets a copy of the production `config.xml`, which sets the element explicitly, so
# `node_off` removes it to run at the compatibility default an upgraded deployment has.
node_on = cluster.add_instance(
    "node_on", main_configs=["configs/readonly_restricts_set_role.xml"]
)
node_off = cluster.add_instance(
    "node_off", main_configs=["configs/readonly_restricts_set_role_absent.xml"]
)


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


def in_session(node, session_id):
    # Every request is a POST so the HTTP method contributes no `readonly` of its own
    # (`setReadOnlyIfHTTPMethodIdempotent` exempts POST), leaving the session's own value as the
    # only source. Reading the roles back needs the same session, which is why this is HTTP.
    def post(sql):
        return node.http_query_and_get_answer_with_error(
            sql, method="POST", params={"session_id": session_id}, user="probe", password="x"
        )

    assert post("SET readonly = 1")[1] is None
    _, error = post("SET ROLE other_role")
    roles, roles_error = post("SELECT currentRoles()")
    assert roles_error is None
    return error, roles.strip()


def test_set_role_refused_mid_session():
    error, roles = in_session(node_on, "refused_mid_session")
    assert error and "READONLY" in error
    assert roles == "['ro_role']"


def test_set_role_allowed_mid_session_at_default_settings():
    error, roles = in_session(node_off, "allowed_mid_session")
    assert error is None
    assert roles == "['other_role']"


def test_http_role_parameter_not_restricted_when_enabled():
    # The `role` URL parameter reaches `Context::setCurrentRoles` without passing through
    # `InterpreterSetRoleQuery`, so the restriction does not cover it.
    def post(sql, **extra):
        return node_on.http_query_and_get_answer_with_error(
            sql,
            method="POST",
            params={"session_id": "http_role_parameter", **extra},
            user="probe",
            password="x",
        )

    assert post("SET readonly = 1")[1] is None
    answer, error = post("SELECT currentRoles()", role="other_role")
    assert error is None
    assert answer.strip() == "['other_role']"
    # The parameter applies to one request, so the session keeps its default role.
    answer, error = post("SELECT currentRoles()")
    assert error is None
    assert answer.strip() == "['ro_role']"
