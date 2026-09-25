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


def test_get_cannot_leave_readonly_mode_in_a_definer_view_body(started_cluster):
    # A `SQL SECURITY DEFINER` body runs in a context built from the global context rather than
    # copied from the request's, so it is the one sub-context of a read-only request that does not
    # inherit the classification by construction. The definer holds `changeable_in_readonly` on
    # `readonly`, so without the classification the body's own `SETTINGS readonly = 0` survives.
    for node in (node_on, node_off):
        node.query("DROP VIEW IF EXISTS default.definer_body_readonly")
        node.query(
            "CREATE VIEW default.definer_body_readonly DEFINER = ro1_kw SQL SECURITY DEFINER "
            "AS SELECT getSetting('readonly') AS r SETTINGS readonly = 0"
        )
        # Same body under the default `INVOKER` security, which reaches the request's context through
        # an ordinary copy. It pins that this arm is about the DEFINER branch and not about the copy.
        node.query("DROP VIEW IF EXISTS default.invoker_body_readonly")
        node.query(
            "CREATE VIEW default.invoker_body_readonly "
            "AS SELECT getSetting('readonly') AS r SETTINGS readonly = 0"
        )

    definer = "SELECT r FROM default.definer_body_readonly"
    invoker = "SELECT r FROM default.invoker_body_readonly"

    output, error = http(node_on, definer, "ro1_kw")
    assert error is None, error
    assert output.strip() == "1"

    output, error = http(node_on, definer, "ro1_kw", method="POST")
    assert error is None, error
    assert output.strip() == "0"

    output, error = http(node_off, definer, "ro1_kw")
    assert error is None, error
    assert output.strip() == "0"

    output, error = http(node_on, invoker, "ro1_kw")
    assert error is None, error
    assert output.strip() == "1"


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


def test_readonly_can_be_tightened(started_cluster):
    assert (
        node_on.query("SET readonly = 1; SELECT getSetting('readonly')", user="ro2").strip() == "1"
    )
    output, error = http(node_on, "SELECT getSetting('readonly')", "ro2", {"readonly": 1})
    assert error is None, error
    assert output.strip() == "1"

    assert REFUSAL in node_off.query_and_get_error("SET readonly = 1", user="ro2")
    output, error = http(node_off, "SELECT getSetting('readonly')", "ro2", {"readonly": 1})
    assert error is not None and REFUSAL in error


def test_readonly_cannot_be_loosened(started_cluster):
    output, error = http(node_on, "SELECT 1", "ro2", {"readonly": 0})
    assert error is not None and REFUSAL in error

    # `readonly = 2` is less restrictive than `readonly = 1`, so this is a loosening too.
    assert REFUSAL in node_on.query_and_get_error(
        "SET readonly = 1; SET readonly = 2", user="ro2"
    )

    # A value the setting cannot take still fails the way it does today, not as a readonly refusal.
    assert "CANNOT_PARSE_INPUT_ASSERTION_FAILED" in node_on.query_and_get_error(
        "SET readonly = 'x'", user="ro2"
    )


def test_tightening_does_not_reopen_the_keyword_escape(started_cluster):
    # `readonly = 2` plus `changeable_in_readonly` on `readonly`: the new permission must not turn
    # into a two-step route out of readonly mode, so the session has to survive both requests.
    output, error = http(node_on, "SET readonly = 1", "ro2_kw", {"session_id": "step_get"})
    assert error is None, error
    output, error = http(node_on, "SET readonly = 0", "ro2_kw", {"session_id": "step_get"})
    assert error is not None and REFUSAL in error
    output, error = http(
        node_on, "SELECT getSetting('readonly')", "ro2_kw", {"session_id": "step_get"}, "POST"
    )
    assert error is None, error
    assert output.strip() == "1"

    # Off the read-only HTTP path both steps are accepted: that is what a `readonly = 1` user
    # holding the same constraint can already do today, and it is the documented residual.
    output, error = http(node_on, "SET readonly = 1", "ro2_kw", {"session_id": "step_post"}, "POST")
    assert error is None, error
    output, error = http(node_on, "SET readonly = 0", "ro2_kw", {"session_id": "step_post"}, "POST")
    assert error is None, error


def test_tightening_does_not_reopen_the_keyword_escape_via_a_switched_profile(started_cluster):
    # The residual is not confined to a user whose own profile declares the keyword: a profile's name
    # is selectable by any session, so `ro2` (which declares nothing) can switch into `kw_only` and
    # pick the constraint up. The switch itself is never constraint-checked, and only the values of
    # the switched-to profile are, so what bounds this route is the refusal of the final step.
    output, error = http(node_on, "SET readonly = 1", "ro2", {"session_id": "prof_get"})
    assert error is None, error
    output, error = http(node_on, "SET profile = 'kw_only'", "ro2", {"session_id": "prof_get"})
    assert error is None, error
    output, error = http(node_on, "SET readonly = 0", "ro2", {"session_id": "prof_get"})
    assert error is not None and REFUSAL in error
    output, error = http(
        node_on, "SELECT getSetting('readonly')", "ro2", {"session_id": "prof_get"}, "POST"
    )
    assert error is None, error
    assert output.strip() == "1"

    # Over POST the whole route completes, which is the residual's real width.
    for sql in ("SET readonly = 1", "SET profile = 'kw_only'", "SET readonly = 0"):
        output, error = http(node_on, sql, "ro2", {"session_id": "prof_post"}, "POST")
        assert error is None, (sql, error)
    output, error = http(
        node_on, "SELECT getSetting('readonly')", "ro2", {"session_id": "prof_post"}, "POST"
    )
    assert error is None, error
    assert output.strip() == "0"

    # The first step is what the key permits, so with the key off `ro2` cannot start the route at
    # all. The route at `readonly = 1` itself is older than this change: `ro1`, which also declares
    # nothing, walks it on both nodes.
    assert REFUSAL in node_off.query_and_get_error("SET readonly = 1", user="ro2")
    for node in (node_on, node_off):
        assert (
            node.query(
                "SET profile = 'kw_only'; SET readonly = 0; SELECT getSetting('readonly')",
                user="ro1",
            ).strip()
            == "0"
        ), node.name
