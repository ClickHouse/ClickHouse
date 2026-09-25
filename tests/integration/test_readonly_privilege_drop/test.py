import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_on = cluster.add_instance(
    "node_on",
    main_configs=["configs/readonly_can_only_be_tightened.xml"],
    user_configs=["configs/users.xml"],
)

# The key is absent here, so it takes its default of false: this node pins the shipped behaviour.
node_off = cluster.add_instance(
    "node_off",
    user_configs=["configs/users.xml"],
)

REFUSAL = "Cannot modify 'readonly' setting in readonly mode"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def http(node, sql, user, params=None, method="GET"):
    """One HTTP request, returned as (output, error). GET is not a mutating method, so it is the
    transport the narrowing applies to; POST is the transport it must leave alone."""
    return node.http_query_and_get_answer_with_error(
        sql, params=params, user=user, method=method
    )


def test_get_cannot_leave_readonly_mode(started_cluster):
    # On node_off the same request succeeds, which is both the shipped behaviour and the guard for
    # this whole file: if `changeable_in_readonly` on `readonly` were not in force, every assertion
    # below would pass without testing anything.
    output, error = http(node_off, "SELECT getSetting('readonly')", "ro1_kw", {"readonly": 0})
    assert error is None, error
    assert output.strip() == "0"

    output, error = http(node_on, "SELECT getSetting('readonly')", "ro1_kw", {"readonly": 0})
    assert error is not None and REFUSAL in error

    # A query that runs in a detached context, which is built before the request is classified.
    output, error = http(
        node_on, "SELECT 42", "ro1_kw", {"readonly": 0, "run_query_in_background": 1}
    )
    assert error is not None and REFUSAL in error


def test_get_cannot_leave_readonly_mode_from_the_query_text(started_cluster):
    # The same change written as SQL rather than as a URL parameter. It is checked through a
    # different `Context::checkSettingsConstraints` overload, and it is the one that would persist
    # into the session and make a later request writable.
    output, error = http(node_on, "SET readonly = 0", "ro1_kw", {"session_id": "text_on"})
    assert error is not None and REFUSAL in error
    output, error = http(
        node_on, "SELECT getSetting('readonly')", "ro1_kw", {"session_id": "text_on"}, "POST"
    )
    assert error is None, error
    assert output.strip() == "1"

    output, error = http(node_off, "SET readonly = 0", "ro1_kw", {"session_id": "text_off"})
    assert error is None, error


def test_get_cannot_reset_readonly_to_its_default(started_cluster):
    # Resetting is checked against the setting's declared default of 0, so it is a loosening too.
    output, error = http(node_on, "SET readonly = DEFAULT", "ro1_kw", {"session_id": "def_on"})
    assert error is not None and REFUSAL in error

    output, error = http(node_off, "SET readonly = DEFAULT", "ro1_kw", {"session_id": "def_off"})
    assert error is None, error
    output, error = http(
        node_off, "SELECT getSetting('readonly')", "ro1_kw", {"session_id": "def_off"}, "POST"
    )
    assert error is None, error
    assert output.strip() == "0"


def test_get_cannot_leave_readonly_mode_in_a_nested_settings_clause(started_cluster):
    # A nested `SETTINGS` clause is clamped rather than refused, and it is applied to a copy of the
    # request's context, so this is the arm that fails if the context copy loses the request's
    # classification: the inner value would become 0.
    query = "SELECT r FROM (SELECT getSetting('readonly') AS r SETTINGS readonly = 0)"
    output, error = http(node_on, query, "ro1_kw")
    assert error is None, error
    assert output.strip() == "1"

    output, error = http(node_off, query, "ro1_kw")
    assert error is None, error
    assert output.strip() == "0"

    output, error = http(node_on, query, "ro1_kw", method="POST")
    assert error is None, error
    assert output.strip() == "0"


def test_post_and_native_keep_the_keyword(started_cluster):
    output, error = http(node_on, "SET readonly = 0", "ro1_kw", {"session_id": "post_on"}, "POST")
    assert error is None, error
    output, error = http(
        node_on, "SELECT getSetting('readonly')", "ro1_kw", {"session_id": "post_on"}, "POST"
    )
    assert error is None, error
    assert output.strip() == "0"

    assert (
        node_on.query("SET readonly = 0; SELECT getSetting('readonly')", user="ro1_kw").strip()
        == "0"
    )


def test_get_keeps_the_keyword_for_other_settings(started_cluster):
    output, error = http(node_on, "SELECT getSetting('max_threads')", "ro1_kw", {"max_threads": 3})
    assert error is None, error
    assert output.strip() == "3"


def test_system_settings_is_unchanged(started_cluster):
    # `system.settings` reports the constraints of a setting without a proposed value and outside any
    # request, so the `readonly` row must read the same with the key on and off, and on both transports.
    query = "SELECT readonly, min, max FROM system.settings WHERE name = 'readonly'"
    for user, expected in (("ro2", "1\t\\N\t\\N"), ("ro1_kw", "0\t\\N\t\\N")):
        for node in (node_on, node_off):
            for method in ("GET", "POST"):
                output, error = http(node, query, user, method=method)
                assert error is None, error
                assert output.strip() == expected, (node.name, user, method, output)
