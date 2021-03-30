import os
import re
import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', user_configs=["configs/users.d/assign_myquota.xml",
                                                          "configs/users.d/drop_default_quota.xml",
                                                          "configs/users.d/quota.xml"])


def check_system_quotas(canonical):
    canonical_tsv = TSV(canonical)
    r = TSV(instance.query("SELECT * FROM system.quotas ORDER BY name"))
    print(("system_quotas: {},\ncanonical: {}".format(r, TSV(canonical_tsv))))
    assert r == canonical_tsv


def system_quota_limits(canonical):
    canonical_tsv = TSV(canonical)
    r = TSV(instance.query("SELECT * FROM system.quota_limits ORDER BY quota_name, duration"))
    print(("system_quota_limits: {},\ncanonical: {}".format(r, TSV(canonical_tsv))))
    assert r == canonical_tsv


def system_quota_usage(canonical):
    canonical_tsv = TSV(canonical)
    query = "SELECT quota_name, quota_key, duration, queries, max_queries, errors, max_errors, result_rows, max_result_rows," \
            "result_bytes, max_result_bytes, read_rows, max_read_rows, read_bytes, max_read_bytes, max_execution_time " \
            "FROM system.quota_usage ORDER BY duration"
    r = TSV(instance.query(query))
    print(("system_quota_usage: {},\ncanonical: {}".format(r, TSV(canonical_tsv))))
    assert r == canonical_tsv


def system_quotas_usage(canonical):
    canonical_tsv = TSV(canonical)
    query = "SELECT quota_name, quota_key, is_current, duration, queries, max_queries, errors, max_errors, result_rows, max_result_rows, " \
            "result_bytes, max_result_bytes, read_rows, max_read_rows, read_bytes, max_read_bytes, max_execution_time " \
            "FROM system.quotas_usage ORDER BY quota_name, quota_key, duration"
    r = TSV(instance.query(query))
    print(("system_quotas_usage: {},\ncanonical: {}".format(r, TSV(canonical_tsv))))
    assert r == canonical_tsv


def copy_quota_xml(local_file_name, reload_immediately=True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    instance.copy_file_to_container(os.path.join(script_dir, local_file_name),
                                    '/etc/clickhouse-server/users.d/quota.xml')
    if reload_immediately:
        instance.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        instance.query("CREATE TABLE test_table(x UInt32) ENGINE = MergeTree ORDER BY tuple()")
        instance.query("INSERT INTO test_table SELECT number FROM numbers(50)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_quotas_and_usage_info():
    try:
        yield
    finally:
        instance.query("DROP QUOTA IF EXISTS qA, qB")
        copy_quota_xml('simpliest.xml')  # To reset usage info.
        copy_quota_xml('normal_limits.xml')


def test_quota_from_users_xml():
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", [31556952],
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quota_usage([["myQuota", "default", 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])
    system_quotas_usage(
        [["myQuota", "default", 1, 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])

    instance.query("SELECT * from test_table")
    system_quota_usage(
        [["myQuota", "default", 31556952, 1, 1000, 0, "\\N", 50, "\\N", 200, "\\N", 50, 1000, 200, "\\N", "\\N"]])

    instance.query("SELECT COUNT() from test_table")
    system_quota_usage(
        [["myQuota", "default", 31556952, 2, 1000, 0, "\\N", 51, "\\N", 208, "\\N", 50, 1000, 200, "\\N", "\\N"]])


def test_simpliest_quota():
    # Simpliest quota doesn't even track usage.
    copy_quota_xml('simpliest.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[]", 0,
                          "['default']", "[]"]])
    system_quota_limits("")
    system_quota_usage(
        [["myQuota", "default", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"]])

    instance.query("SELECT * from test_table")
    system_quota_usage(
        [["myQuota", "default", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"]])


def test_tracking_quota():
    # Now we're tracking usage.
    copy_quota_xml('tracking.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[31556952]",
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"]])
    system_quota_usage([["myQuota", "default", 31556952, 0, "\\N", 0, "\\N", 0, "\\N", 0, "\\N", 0, "\\N", 0, "\\N", "\\N"]])

    instance.query("SELECT * from test_table")
    system_quota_usage(
        [["myQuota", "default", 31556952, 1, "\\N", 0, "\\N", 50, "\\N", 200, "\\N", 50, "\\N", 200, "\\N", "\\N"]])

    instance.query("SELECT COUNT() from test_table")
    system_quota_usage(
        [["myQuota", "default", 31556952, 2, "\\N", 0, "\\N", 51, "\\N", 208, "\\N", 50, "\\N", 200, "\\N", "\\N"]])


def test_exceed_quota():
    # Change quota, now the limits are tiny so we will exceed the quota.
    copy_quota_xml('tiny_limits.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[31556952]",
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1, 1, 1, "\\N", 1, "\\N", "\\N"]])
    system_quota_usage([["myQuota", "default", 31556952, 0, 1, 0, 1, 0, 1, 0, "\\N", 0, 1, 0, "\\N", "\\N"]])

    assert re.search("Quota.*has\ been\ exceeded", instance.query_and_get_error("SELECT * from test_table"))
    system_quota_usage([["myQuota", "default", 31556952, 1, 1, 1, 1, 0, 1, 0, "\\N", 50, 1, 0, "\\N", "\\N"]])

    # Change quota, now the limits are enough to execute queries.
    copy_quota_xml('normal_limits.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[31556952]",
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quota_usage([["myQuota", "default", 31556952, 1, 1000, 1, "\\N", 0, "\\N", 0, "\\N", 50, 1000, 0, "\\N", "\\N"]])

    instance.query("SELECT * from test_table")
    system_quota_usage(
        [["myQuota", "default", 31556952, 2, 1000, 1, "\\N", 50, "\\N", 200, "\\N", 100, 1000, 200, "\\N", "\\N"]])


def test_add_remove_interval():
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", [31556952],
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quota_usage([["myQuota", "default", 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])

    # Add interval.
    copy_quota_xml('two_intervals.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']",
                          "[31556952,63113904]", 0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"],
                         ["myQuota", 63113904, 1, "\\N", "\\N", "\\N", 30000, "\\N", 20000, 120]])
    system_quota_usage([["myQuota", "default", 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"],
                        ["myQuota", "default", 63113904, 0, "\\N", 0, "\\N", 0, "\\N", 0, 30000, 0, "\\N", 0, 20000, 120]])

    instance.query("SELECT * from test_table")
    system_quota_usage(
        [["myQuota", "default", 31556952, 1, 1000, 0, "\\N", 50, "\\N", 200, "\\N", 50, 1000, 200, "\\N", "\\N"],
         ["myQuota", "default", 63113904, 1, "\\N", 0, "\\N", 50, "\\N", 200, 30000, 50, "\\N", 200, 20000, 120]])

    # Remove interval.
    copy_quota_xml('normal_limits.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", [31556952],
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quota_usage(
        [["myQuota", "default", 31556952, 1, 1000, 0, "\\N", 50, "\\N", 200, "\\N", 50, 1000, 200, "\\N", "\\N"]])

    instance.query("SELECT * from test_table")
    system_quota_usage(
        [["myQuota", "default", 31556952, 2, 1000, 0, "\\N", 100, "\\N", 400, "\\N", 100, 1000, 400, "\\N", "\\N"]])

    # Remove all intervals.
    copy_quota_xml('simpliest.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[]", 0,
                          "['default']", "[]"]])
    system_quota_limits("")
    system_quota_usage(
        [["myQuota", "default", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"]])

    instance.query("SELECT * from test_table")
    system_quota_usage(
        [["myQuota", "default", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", "\\N"]])

    # Add one interval back.
    copy_quota_xml('normal_limits.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", [31556952],
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quota_usage([["myQuota", "default", 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])


def test_add_remove_quota():
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", [31556952],
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quotas_usage(
        [["myQuota", "default", 1, 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])

    # Add quota.
    copy_quota_xml('two_quotas.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[31556952]",
                          0, "['default']", "[]"],
                         ["myQuota2", "4590510c-4d13-bf21-ec8a-c2187b092e73", "users.xml", "['client_key','user_name']",
                          "[3600,2629746]", 0, "[]", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"],
                         ["myQuota2", 3600, 1, "\\N", "\\N", 4000, 400000, 4000, 400000, 60],
                         ["myQuota2", 2629746, 0, "\\N", "\\N", "\\N", "\\N", "\\N", "\\N", 1800]])
    system_quotas_usage(
        [["myQuota", "default", 1, 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])

    # Drop quota.
    copy_quota_xml('normal_limits.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[31556952]",
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quotas_usage(
        [["myQuota", "default", 1, 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])

    # Drop all quotas.
    copy_quota_xml('no_quotas.xml')
    check_system_quotas("")
    system_quota_limits("")
    system_quotas_usage("")

    # Add one quota back.
    copy_quota_xml('normal_limits.xml')
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[31556952]",
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])
    system_quotas_usage(
        [["myQuota", "default", 1, 31556952, 0, 1000, 0, "\\N", 0, "\\N", 0, "\\N", 0, 1000, 0, "\\N", "\\N"]])


def test_reload_users_xml_by_timer():
    check_system_quotas([["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", "['user_name']", "[31556952]",
                          0, "['default']", "[]"]])
    system_quota_limits([["myQuota", 31556952, 0, 1000, "\\N", "\\N", "\\N", 1000, "\\N", "\\N"]])

    time.sleep(1)  # The modification time of the 'quota.xml' file should be different,
    # because config files are reload by timer only when the modification time is changed.
    copy_quota_xml('tiny_limits.xml', reload_immediately=False)
    assert_eq_with_retry(instance, "SELECT * FROM system.quotas", [
        ["myQuota", "e651da9c-a748-8703-061a-7e5e5096dae7", "users.xml", ['user_name'], "[31556952]", 0, "['default']",
         "[]"]])
    assert_eq_with_retry(instance, "SELECT * FROM system.quota_limits",
                         [["myQuota", 31556952, 0, 1, 1, 1, "\\N", 1, "\\N", "\\N"]])


def test_dcl_introspection():
    assert instance.query("SHOW QUOTAS") == "myQuota\n"
    assert instance.query(
        "SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY user_name FOR INTERVAL 1 year MAX queries = 1000, read_rows = 1000 TO default\n"
    assert instance.query(
        "SHOW CREATE QUOTAS") == "CREATE QUOTA myQuota KEYED BY user_name FOR INTERVAL 1 year MAX queries = 1000, read_rows = 1000 TO default\n"
    assert re.match(
        "myQuota\\tdefault\\t.*\\t31556952\\t0\\t1000\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t1000\\t0\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query("SELECT * from test_table")
    assert re.match(
        "myQuota\\tdefault\\t.*\\t31556952\\t1\\t1000\\t0\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t1000\\t200\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    expected_access = "CREATE QUOTA myQuota KEYED BY user_name FOR INTERVAL 1 year MAX queries = 1000, read_rows = 1000 TO default\n"
    assert expected_access in instance.query("SHOW ACCESS")

    # Add interval.
    copy_quota_xml('two_intervals.xml')
    assert instance.query("SHOW QUOTAS") == "myQuota\n"
    assert instance.query(
        "SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY user_name FOR INTERVAL 1 year MAX queries = 1000, read_rows = 1000, FOR RANDOMIZED INTERVAL 2 year MAX result_bytes = 30000, read_bytes = 20000, execution_time = 120 TO default\n"
    assert re.match(
        "myQuota\\tdefault\\t.*\\t31556952\\t1\\t1000\\t0\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t1000\\t200\\t\\\\N\\t.*\\t\\\\N\n"
        "myQuota\\tdefault\\t.*\\t63113904\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t30000\\t0\\t\\\\N\\t0\\t20000\\t.*\\t120",
        instance.query("SHOW QUOTA"))

    # Drop interval, add quota.
    copy_quota_xml('two_quotas.xml')
    assert instance.query("SHOW QUOTAS") == "myQuota\nmyQuota2\n"
    assert instance.query(
        "SHOW CREATE QUOTA myQuota") == "CREATE QUOTA myQuota KEYED BY user_name FOR INTERVAL 1 year MAX queries = 1000, read_rows = 1000 TO default\n"
    assert instance.query(
        "SHOW CREATE QUOTA myQuota2") == "CREATE QUOTA myQuota2 KEYED BY client_key, user_name FOR RANDOMIZED INTERVAL 1 hour MAX result_rows = 4000, result_bytes = 400000, read_rows = 4000, read_bytes = 400000, execution_time = 60, FOR INTERVAL 1 month MAX execution_time = 1800\n"
    assert instance.query(
        "SHOW CREATE QUOTAS") == "CREATE QUOTA myQuota KEYED BY user_name FOR INTERVAL 1 year MAX queries = 1000, read_rows = 1000 TO default\n" \
                                 "CREATE QUOTA myQuota2 KEYED BY client_key, user_name FOR RANDOMIZED INTERVAL 1 hour MAX result_rows = 4000, result_bytes = 400000, read_rows = 4000, read_bytes = 400000, execution_time = 60, FOR INTERVAL 1 month MAX execution_time = 1800\n"
    assert re.match(
        "myQuota\\tdefault\\t.*\\t31556952\\t1\\t1000\\t0\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t1000\\t200\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    # Drop all quotas.
    copy_quota_xml('no_quotas.xml')
    assert instance.query("SHOW QUOTAS") == ""
    assert instance.query("SHOW CREATE QUOTA") == ""
    assert instance.query("SHOW QUOTA") == ""


def test_dcl_management():
    copy_quota_xml('no_quotas.xml')
    assert instance.query("SHOW QUOTA") == ""

    instance.query("CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER")
    assert instance.query(
        "SHOW CREATE QUOTA qA") == "CREATE QUOTA qA FOR INTERVAL 5 quarter MAX queries = 123 TO default\n"
    assert re.match(
        "qA\\t\\t.*\\t39446190\\t0\\t123\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query("SELECT * from test_table")
    assert re.match(
        "qA\\t\\t.*\\t39446190\\t1\\t123\\t0\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query(
        "ALTER QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 321, MAX ERRORS 10, FOR INTERVAL 0.5 HOUR MAX EXECUTION TIME 0.5")
    assert instance.query(
        "SHOW CREATE QUOTA qA") == "CREATE QUOTA qA FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default\n"
    assert re.match(
        "qA\\t\\t.*\\t1800\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t.*\\t0.5\n"
        "qA\\t\\t.*\\t39446190\\t1\\t321\\t0\\t10\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query("SELECT * from test_table")
    assert re.match(
        "qA\\t\\t.*\\t1800\\t1\\t\\\\N\\t0\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t.*\\t0.5\n"
        "qA\\t\\t.*\\t39446190\\t2\\t321\\t0\\t10\\t100\\t\\\\N\\t400\\t\\\\N\\t100\\t\\\\N\\t400\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query(
        "ALTER QUOTA qA FOR INTERVAL 15 MONTH NO LIMITS, FOR RANDOMIZED INTERVAL 16 MONTH TRACKING ONLY, FOR INTERVAL 1800 SECOND NO LIMITS")
    assert re.match(
        "qA\\t\\t.*\\t42075936\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t0\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query("SELECT * from test_table")
    assert re.match(
        "qA\\t\\t.*\\t42075936\\t1\\t\\\\N\\t0\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query("ALTER QUOTA qA RENAME TO qB")
    assert instance.query(
        "SHOW CREATE QUOTA qB") == "CREATE QUOTA qB FOR RANDOMIZED INTERVAL 16 month TRACKING ONLY TO default\n"
    assert re.match(
        "qB\\t\\t.*\\t42075936\\t1\\t\\\\N\\t0\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t50\\t\\\\N\\t200\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query("SELECT * from test_table")
    assert re.match(
        "qB\\t\\t.*\\t42075936\\t2\\t\\\\N\\t0\\t\\\\N\\t100\\t\\\\N\\t400\\t\\\\N\\t100\\t\\\\N\\t400\\t\\\\N\\t.*\\t\\\\N\n",
        instance.query("SHOW QUOTA"))

    instance.query("DROP QUOTA qB")
    assert instance.query("SHOW QUOTA") == ""


def test_users_xml_is_readonly():
    assert re.search("storage is readonly", instance.query_and_get_error("DROP QUOTA myQuota"))
