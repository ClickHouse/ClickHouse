import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", user_configs=["configs/normal_settings.xml"])

# A separate node which *starts* with the constraints in place. That matters: the constraints which
# used to survive a reload are the ones applied to the global context at startup, so a constraint
# added to a running server and then removed again would not reproduce the problem.
node_with_constraints = cluster.add_instance(
    "node_with_constraints", user_configs=["configs/constrained_settings.xml"]
)

CONSTRAINED_CONFIG_PATH = "/etc/clickhouse-server/users.d/constrained_settings.xml"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_to_normal_settings_after_test():
    try:
        node.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs/normal_settings.xml"),
            "/etc/clickhouse-server/users.d/z.xml",
        )
        node.query("SYSTEM RELOAD CONFIG")
        node_with_constraints.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs/constrained_settings.xml"),
            CONSTRAINED_CONFIG_PATH,
        )
        node_with_constraints.query("SYSTEM RELOAD CONFIG")
        yield
    finally:
        pass


def test_force_reload():
    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
    assert node.query("SELECT getSetting('alter_sync')") == "2\n"

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/changed_settings.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "20000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "nearest_hostname\n"
    assert node.query("SELECT getSetting('alter_sync')") == "0\n"


def test_reload_on_timeout():
    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
    assert node.query("SELECT getSetting('alter_sync')") == "2\n"

    time.sleep(1)  # The modification time of the 'z.xml' file should be different,
    # because config files are reload by timer only when the modification time is changed.
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/changed_settings.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )

    assert_eq_with_retry(node, "SELECT getSetting('max_memory_usage')", "20000000000")
    assert_eq_with_retry(
        node, "SELECT getSetting('load_balancing')", "nearest_hostname"
    )
    assert_eq_with_retry(node, "SELECT getSetting('alter_sync')", "0")


def test_unknown_setting_force_reload():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/unknown_setting.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )

    error_message = "Setting xyz is neither a builtin setting nor started with the prefix 'custom_' registered for user-defined settings"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
    assert node.query("SELECT getSetting('alter_sync')") == "2\n"


def test_unknown_setting_reload_on_timeout():
    time.sleep(1)  # The modification time of the 'z.xml' file should be different,
    # because config files are reload by timer only when the modification time is changed.
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/unknown_setting.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )

    error_message = "Setting xyz is neither a builtin setting nor started with the prefix 'custom_' registered for user-defined settings"
    assert_logs_contain_with_retry(node, error_message)

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
    assert node.query("SELECT getSetting('alter_sync')") == "2\n"


def test_unexpected_setting_int():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/unexpected_setting_int.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )
    error_message = "Cannot parse"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
    assert node.query("SELECT getSetting('alter_sync')") == "2\n"


def test_unexpected_setting_enum():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/unexpected_setting_int.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )
    error_message = "Cannot parse"
    assert error_message in node.query_and_get_error("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"
    assert node.query("SELECT getSetting('load_balancing')") == "first_or_random\n"
    assert node.query("SELECT getSetting('alter_sync')") == "2\n"


def remove_the_constraints():
    node_with_constraints.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/unconstrained_settings.xml"),
        CONSTRAINED_CONFIG_PATH,
    )


def get_constraints(setting_name, user="default"):
    return node_with_constraints.query(
        "SELECT min, max, readonly FROM system.settings WHERE name='"
        + setting_name
        + "'",
        user=user,
    )


def test_removing_constraint_from_default_profile():
    uptime_before = int(node_with_constraints.query("SELECT uptime()"))

    # The constraints are in effect. Adding them was never broken, but assert it so that a fix which
    # simply stops applying constraints does not pass.
    assert get_constraints("alter_sync") == "\\N\t\\N\t1\n"
    assert get_constraints("max_memory_usage") == "5000000000\t20000000000\t0\n"
    assert (
        "Setting alter_sync should not be changed"
        in node_with_constraints.query_and_get_error("SELECT 1 SETTINGS alter_sync = 0")
    )
    assert (
        "Setting max_memory_usage shouldn't be less than 5000000000"
        in node_with_constraints.query_and_get_error(
            "SELECT 1 SETTINGS max_memory_usage = 100"
        )
    )

    # Now remove them. Every query() opens a new connection, so these are all new sessions.
    remove_the_constraints()
    node_with_constraints.query("SYSTEM RELOAD CONFIG")

    assert get_constraints("alter_sync") == "\\N\t\\N\t0\n"
    assert get_constraints("max_memory_usage") == "\\N\t\\N\t0\n"
    assert node_with_constraints.query("SELECT 1 SETTINGS alter_sync = 0") == "1\n"
    assert (
        node_with_constraints.query("SELECT 1 SETTINGS max_memory_usage = 100000000")
        == "1\n"
    )

    # The server must not have been restarted, otherwise the assertions above prove nothing.
    assert int(node_with_constraints.query("SELECT uptime()")) >= uptime_before


def test_removing_constraint_from_default_profile_on_timeout():
    assert get_constraints("alter_sync") == "\\N\t\\N\t1\n"

    time.sleep(1)  # The modification time of the config file should be different,
    # because config files are reload by timer only when the modification time is changed.
    remove_the_constraints()

    assert_eq_with_retry(
        node_with_constraints,
        "SELECT readonly FROM system.settings WHERE name='alter_sync'",
        "0",
    )


def test_removing_constraint_from_non_default_profile():
    # A constraint which lives only in a profile assigned to a specific user is not part of the
    # snapshot taken from the global context at startup, so removing it always worked. This pins that
    # behaviour, and the asymmetry with the default profile which made the problem hard to spot.
    assert (
        get_constraints("max_threads", user="user_with_own_profile") == "\\N\t\\N\t1\n"
    )

    remove_the_constraints()
    node_with_constraints.query("SYSTEM RELOAD CONFIG")

    assert (
        get_constraints("max_threads", user="user_with_own_profile") == "\\N\t\\N\t0\n"
    )


@pytest.mark.xfail(
    reason="Removing the VALUE of a setting from the default profile is stuck until restart, by the "
    "same stale global snapshot which used to keep removed constraints alive: `Context::createCopy` "
    "copies the settings of the global context and `Context::setUser` applies the profile changes on "
    "top of them. Fixing it needs a snapshot of the settings taken before the system profile is "
    "applied at startup, which is a separate change."
)
def test_removing_setting_falls_back_to_default():
    assert node.query("SELECT getSetting('max_memory_usage')") == "10000000000\n"

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/removed_settings.xml"),
        "/etc/clickhouse-server/users.d/z.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")

    assert node.query("SELECT getSetting('max_memory_usage')") == "0\n"
