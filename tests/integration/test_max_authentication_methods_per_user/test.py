import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

limited_node = cluster.add_instance(
    "limited_node",
    main_configs=["configs/max_auth_limited.xml"],
    stay_alive=True,
)

default_node = cluster.add_instance(
    "default_node",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


expected_error = "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user"

ssh_key_type = "ssh-ed25519"

ssh_keys = [
    "AAAAC3NzaC1lZDI1NTE5AAAAIJGzPKVGIUBgsG/kkmEYCZIY99PI5KdHeA5Cibrv9HgW",
    "AAAAC3NzaC1lZDI1NTE5AAAAIPf2G0r1/KLxLhiP3DUGj0rMIDZ+rf/BnMANAm8DnugR",
    "AAAAC3NzaC1lZDI1NTE5AAAAILc1kfwrTHeptNprRnKZuYKP6IT3+LZXza8MKmCcJLVh",
]


def test_create(started_cluster):

    assert expected_error in limited_node.query_and_get_error(
        "CREATE USER u_max_authentication_methods IDENTIFIED BY '1', BY '2', BY '3'"
    )

    assert expected_error not in limited_node.query_and_get_answer_with_error(
        "CREATE USER u_max_authentication_methods IDENTIFIED BY '1', BY '2'"
    )

    limited_node.query("DROP USER u_max_authentication_methods")


def test_alter(started_cluster):
    limited_node.query("CREATE USER u_max_authentication_methods IDENTIFIED BY '1'")

    assert expected_error in limited_node.query_and_get_error(
        "ALTER USER u_max_authentication_methods ADD IDENTIFIED BY '2', BY '3'"
    )

    assert expected_error in limited_node.query_and_get_error(
        "ALTER USER u_max_authentication_methods IDENTIFIED BY '3', BY '4', BY '5'"
    )

    assert expected_error not in limited_node.query_and_get_answer_with_error(
        "ALTER USER u_max_authentication_methods ADD IDENTIFIED BY '2'"
    )

    assert expected_error not in limited_node.query_and_get_answer_with_error(
        "ALTER USER u_max_authentication_methods IDENTIFIED BY '2', BY '3'"
    )

    limited_node.query("DROP USER u_max_authentication_methods")


def test_create_ssh_key_split(started_cluster):
    limited_node.query("DROP USER IF EXISTS u_ssh_split")

    keys_in_one_method = ", ".join(
        f"KEY '{key}' TYPE '{ssh_key_type}'" for key in ssh_keys
    )
    separate_ssh_methods = ", ".join(
        f"ssh_key BY KEY '{key}' TYPE '{ssh_key_type}'" for key in ssh_keys
    )
    two_keys_in_one_method = ", ".join(
        f"KEY '{key}' TYPE '{ssh_key_type}'" for key in ssh_keys[:2]
    )

    assert expected_error in limited_node.query_and_get_error(
        f"CREATE USER u_ssh_split IDENTIFIED WITH ssh_key BY {keys_in_one_method}"
    )

    assert expected_error in limited_node.query_and_get_error(
        f"CREATE USER u_ssh_split IDENTIFIED WITH {separate_ssh_methods}"
    )

    assert expected_error not in limited_node.query_and_get_answer_with_error(
        f"CREATE USER u_ssh_split IDENTIFIED WITH ssh_key BY {two_keys_in_one_method}"
    )

    limited_node.query("DROP USER u_ssh_split")


def test_alter_add_ssh_key(started_cluster):
    limited_node.query("DROP USER IF EXISTS u_ssh_alter")

    two_keys_in_one_method = ", ".join(
        f"KEY '{key}' TYPE '{ssh_key_type}'" for key in ssh_keys[:2]
    )

    limited_node.query(
        f"CREATE USER u_ssh_alter IDENTIFIED WITH ssh_key BY {two_keys_in_one_method}"
    )

    assert expected_error in limited_node.query_and_get_error(
        "ALTER USER u_ssh_alter ADD IDENTIFIED WITH plaintext_password BY '1'"
    )

    limited_node.query("DROP USER u_ssh_alter")


def get_query_with_multiple_identified_with(
    operation, username, identified_with_count, add_operation=""
):
    identified_clauses = ", ".join(["BY '1'" for _ in range(identified_with_count)])
    query = (
        f"{operation} USER {username} {add_operation} IDENTIFIED {identified_clauses}"
    )
    return query


def test_create_default_setting(started_cluster):
    expected_error = "User can not be created/updated because it exceeds the allowed quantity of authentication methods per user"

    query_exceeds = get_query_with_multiple_identified_with(
        "CREATE", "u_max_authentication_methods", 101
    )

    assert expected_error in default_node.query_and_get_error(query_exceeds)

    query_not_exceeds = get_query_with_multiple_identified_with(
        "CREATE", "u_max_authentication_methods", 100
    )

    assert expected_error not in default_node.query_and_get_answer_with_error(
        query_not_exceeds
    )

    default_node.query("DROP USER u_max_authentication_methods")


def test_alter_default_setting(started_cluster):
    default_node.query("CREATE USER u_max_authentication_methods IDENTIFIED BY '1'")

    query_add_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 100, "ADD"
    )

    assert expected_error in default_node.query_and_get_error(query_add_exceeds)

    query_replace_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 101
    )

    assert expected_error in default_node.query_and_get_error(query_replace_exceeds)

    query_add_not_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 99, "ADD"
    )

    assert expected_error not in default_node.query_and_get_answer_with_error(
        query_add_not_exceeds
    )

    query_replace_not_exceeds = get_query_with_multiple_identified_with(
        "ALTER", "u_max_authentication_methods", 100
    )

    assert expected_error not in default_node.query_and_get_answer_with_error(
        query_replace_not_exceeds
    )

    default_node.query("DROP USER u_max_authentication_methods")


def test_alter_prunes_expired_methods_before_the_limit_check(started_cluster):
    # Every `ALTER USER` drops the authentication methods whose `VALID UNTIL` deadline has already
    # passed *before* the new methods are counted against `max_authentication_methods_per_user`
    # (2 on this node). This is what lets a short-lived credential be rotated indefinitely on a user
    # that is already at the limit: without the pruning, or with it applied after the count check,
    # the second rotation below would be rejected.
    limited_node.query("DROP USER IF EXISTS u_rotate_expired")
    limited_node.query(
        "CREATE USER u_rotate_expired IDENTIFIED WITH plaintext_password BY 'live'"
    )
    limited_node.query(
        "ALTER USER u_rotate_expired ADD IDENTIFIED WITH plaintext_password BY 'token_1' "
        "VALID UNTIL '2020-01-01 00:00:00 UTC'"
    )
    # Two methods: the user is at the limit, one of them is already expired.
    assert (
        limited_node.query(
            "SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'u_rotate_expired'"
        )
        == "[0,1577836800]\n"
    )

    # Rotating the token is accepted: the expired one is dropped first, so the new one fits.
    assert expected_error not in limited_node.query_and_get_answer_with_error(
        "ALTER USER u_rotate_expired ADD IDENTIFIED WITH plaintext_password BY 'token_2' "
        "VALID UNTIL '2100-01-01 00:00:00 UTC'"
    )
    assert (
        limited_node.query(
            "SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'u_rotate_expired'"
        )
        == "[0,4102444800]\n"
    )
    assert (
        limited_node.query("SELECT 1", user="u_rotate_expired", password="token_2")
        == "1\n"
    )
    assert "token_1" not in limited_node.query("SHOW CREATE USER u_rotate_expired")

    # The limit still applies to the methods that survive: both remaining ones are valid, so a
    # third one does not fit.
    assert expected_error in limited_node.query_and_get_error(
        "ALTER USER u_rotate_expired ADD IDENTIFIED WITH plaintext_password BY 'token_3' "
        "VALID UNTIL '2100-01-01 00:00:00 UTC'"
    )
    assert (
        limited_node.query(
            "SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'u_rotate_expired'"
        )
        == "[0,4102444800]\n"
    )

    limited_node.query("DROP USER u_rotate_expired")


def test_restart_does_not_prune_expired_methods(started_cluster):
    # Pruning happens only on a real `ALTER USER`; loading the stored `ATTACH USER` definition at
    # startup must materialize exactly what was written, so a user with one live and one expired
    # method keeps both across a restart.
    limited_node.query("DROP USER IF EXISTS u_restart_expired")
    # A method added by the statement itself is never pruned, so this stores an already expired one.
    limited_node.query(
        "CREATE USER u_restart_expired IDENTIFIED WITH plaintext_password BY 'live', "
        "plaintext_password BY 'old' VALID UNTIL '2020-01-01 00:00:00 UTC'"
    )
    query = "SELECT arrayMap(x -> toUInt32(x), valid_until) FROM system.users WHERE name = 'u_restart_expired'"
    assert limited_node.query(query) == "[0,1577836800]\n"

    limited_node.restart_clickhouse()

    assert limited_node.query(query) == "[0,1577836800]\n"
    assert (
        limited_node.query("SELECT 1", user="u_restart_expired", password="live")
        == "1\n"
    )

    # The next write prunes it.
    limited_node.query("ALTER USER u_restart_expired DEFAULT ROLE NONE")
    assert limited_node.query(query) == "[0]\n"

    limited_node.query("DROP USER u_restart_expired")
