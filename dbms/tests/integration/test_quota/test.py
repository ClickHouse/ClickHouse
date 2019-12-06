import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import os
import re
import time

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir="configs")

query_from_system_quotas = "SELECT * FROM system.quotas ORDER BY name";

query_from_system_quota_usage = "SELECT id, key, duration, "\
                                 "queries, errors, result_rows, result_bytes, read_rows, read_bytes "\
                                 "FROM system.quota_usage ORDER BY id, key, duration";

def system_quotas():
    return instance.query(query_from_system_quotas).rstrip('\n')

def system_quota_usage():
    return instance.query(query_from_system_quota_usage).rstrip('\n')


def copy_quota_xml(local_file_name, reload_immediately = True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    instance.copy_file_to_container(os.path.join(script_dir, local_file_name), '/etc/clickhouse-server/users.d/quota.xml')
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
        copy_quota_xml('simpliest.xml') # To reset usage info.
        copy_quota_xml('normal_limits.xml')


def test_quota_from_users_xml():
    assert instance.query("SELECT currentQuota()") == "myQuota\n"
    assert instance.query("SELECT currentQuotaID()") == "e651da9c-a748-8703-061a-7e5e5096dae7\n"
    assert instance.query("SELECT currentQuotaKey()") == "default\n"
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200"

    instance.query("SELECT COUNT() from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t0\t51\t208\t50\t200"


def test_simpliest_quota():
    # Simpliest quota doesn't even track usage.
    copy_quota_xml('simpliest.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"

    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"


def test_tracking_quota():
    # Now we're tracking usage.
    copy_quota_xml('tracking.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[0]\t[0]\t[0]\t[0]\t[0]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200"

    instance.query("SELECT COUNT() from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t0\t51\t208\t50\t200"


def test_exceed_quota():
    # Change quota, now the limits are tiny so we will exceed the quota.
    copy_quota_xml('tiny_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1]\t[1]\t[1]\t[0]\t[1]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    assert re.search("Quota.*has\ been\ exceeded", instance.query_and_get_error("SELECT * from test_table"))
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t1\t0\t0\t50\t0"

    # Change quota, now the limits are enough to execute queries.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t1\t0\t0\t50\t0"
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t1\t50\t200\t100\t200"


def test_add_remove_interval():
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    # Add interval.
    copy_quota_xml('two_intervals.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952,63113904]\t[0,1]\t[1000,0]\t[0,0]\t[0,0]\t[0,30000]\t[1000,0]\t[0,20000]\t[0,120]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0\n"\
                                   "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t63113904\t0\t0\t0\t0\t0\t0"
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200\n"\
                                   "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t63113904\t1\t0\t50\t200\t50\t200"

    # Remove interval.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200"
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t0\t100\t400\t100\t400"

    # Remove all intervals.
    copy_quota_xml('simpliest.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"

    # Add one interval back.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"


def test_add_remove_quota():
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    # Add quota.
    copy_quota_xml('two_quotas.xml')
    assert system_quotas() ==\
           "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]\n"\
           "myQuota2\t4590510c-4d13-bf21-ec8a-c2187b092e73\tusers.xml\tclient key or user name\t[]\t0\t[]\t[3600,2629746]\t[1,0]\t[0,0]\t[0,0]\t[4000,0]\t[400000,0]\t[4000,0]\t[400000,0]\t[60,1800]"

    # Drop quota.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"

    # Drop all quotas.
    copy_quota_xml('no_quotas.xml')
    assert system_quotas() == ""
    assert system_quota_usage() == ""

    # Add one quota back.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"


def test_reload_users_xml_by_timer():
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"

    time.sleep(1) # The modification time of the 'quota.xml' file should be different,
                  # because config files are reload by timer only when the modification time is changed.
    copy_quota_xml('tiny_limits.xml', reload_immediately=False)
    assert_eq_with_retry(instance, query_from_system_quotas, "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1]\t[1]\t[1]\t[0]\t[1]\t[0]\t[0]")


def test_dcl_introspection():
    assert instance.query("SHOW QUOTAS") == "myQuota\n"
    assert instance.query("SHOW CREATE QUOTA myQuota") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' FOR INTERVAL 1 YEAR MAX QUERIES = 1000, MAX READ ROWS = 1000 TO default\n"
    expected_usage = "myQuota key=\\\\'default\\\\' interval=\[.*\] queries=0/1000 errors=0 result_rows=0 result_bytes=0 read_rows=0/1000 read_bytes=0 execution_time=0"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE CURRENT"))
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE ALL"))

    instance.query("SELECT * from test_table")
    expected_usage = "myQuota key=\\\\'default\\\\' interval=\[.*\] queries=1/1000 errors=0 result_rows=50 result_bytes=200 read_rows=50/1000 read_bytes=200 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))

    # Add interval.
    copy_quota_xml('two_intervals.xml')
    assert instance.query("SHOW QUOTAS") == "myQuota\n"
    assert instance.query("SHOW CREATE QUOTA myQuota") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' FOR INTERVAL 1 YEAR MAX QUERIES = 1000, MAX READ ROWS = 1000, FOR RANDOMIZED INTERVAL 2 YEAR MAX RESULT BYTES = 30000, MAX READ BYTES = 20000, MAX EXECUTION TIME = 120 TO default\n"
    expected_usage = "myQuota key=\\\\'default\\\\' interval=\[.*\] queries=1/1000 errors=0 result_rows=50 result_bytes=200 read_rows=50/1000 read_bytes=200 execution_time=.*\n"\
                     "myQuota key=\\\\'default\\\\' interval=\[.*\] queries=0 errors=0 result_rows=0 result_bytes=0/30000 read_rows=0 read_bytes=0/20000 execution_time=0/120"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))

    # Drop interval, add quota.
    copy_quota_xml('two_quotas.xml')
    assert instance.query("SHOW QUOTAS") == "myQuota\nmyQuota2\n"
    assert instance.query("SHOW CREATE QUOTA myQuota") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' FOR INTERVAL 1 YEAR MAX QUERIES = 1000, MAX READ ROWS = 1000 TO default\n"
    assert instance.query("SHOW CREATE QUOTA myQuota2") == "CREATE QUOTA myQuota2 KEYED BY \\'client key or user name\\' FOR RANDOMIZED INTERVAL 1 HOUR MAX RESULT ROWS = 4000, MAX RESULT BYTES = 400000, MAX READ ROWS = 4000, MAX READ BYTES = 400000, MAX EXECUTION TIME = 60, FOR INTERVAL 1 MONTH MAX EXECUTION TIME = 1800\n"
    expected_usage = "myQuota key=\\\\'default\\\\' interval=\[.*\] queries=1/1000 errors=0 result_rows=50 result_bytes=200 read_rows=50/1000 read_bytes=200 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))


def test_dcl_management():
    copy_quota_xml('no_quotas.xml')
    assert instance.query("SHOW QUOTAS") == ""
    assert instance.query("SHOW QUOTA USAGE") == ""
    
    instance.query("CREATE QUOTA qA FOR INTERVAL 15 MONTH SET MAX QUERIES = 123 TO CURRENT_USER")
    assert instance.query("SHOW QUOTAS") == "qA\n"
    assert instance.query("SHOW CREATE QUOTA qA") == "CREATE QUOTA qA KEYED BY \\'none\\' FOR INTERVAL 5 QUARTER MAX QUERIES = 123 TO default\n"
    expected_usage = "qA key=\\\\'\\\\' interval=\[.*\] queries=0/123 errors=0 result_rows=0 result_bytes=0 read_rows=0 read_bytes=0 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))
   
    instance.query("SELECT * from test_table")
    expected_usage = "qA key=\\\\'\\\\' interval=\[.*\] queries=1/123 errors=0 result_rows=50 result_bytes=200 read_rows=50 read_bytes=200 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))

    instance.query("ALTER QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES = 321, MAX ERRORS = 10, FOR INTERVAL 0.5 HOUR MAX EXECUTION TIME = 0.5")
    assert instance.query("SHOW CREATE QUOTA qA") == "CREATE QUOTA qA KEYED BY \\'none\\' FOR INTERVAL 30 MINUTE MAX EXECUTION TIME = 0.5, FOR INTERVAL 5 QUARTER MAX QUERIES = 321, MAX ERRORS = 10 TO default\n"
    expected_usage = "qA key=\\\\'\\\\' interval=\[.*\] queries=0 errors=0 result_rows=0 result_bytes=0 read_rows=0 read_bytes=0 execution_time=.*/0.5\n"\
                     "qA key=\\\\'\\\\' interval=\[.*\] queries=1/321 errors=0/10 result_rows=50 result_bytes=200 read_rows=50 read_bytes=200 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))

    instance.query("ALTER QUOTA qA FOR INTERVAL 15 MONTH UNSET TRACKING, FOR RANDOMIZED INTERVAL 16 MONTH SET TRACKING, FOR INTERVAL 1800 SECOND UNSET TRACKING")
    assert instance.query("SHOW CREATE QUOTA qA") == "CREATE QUOTA qA KEYED BY \\'none\\' FOR RANDOMIZED INTERVAL 16 MONTH TRACKING TO default\n"
    expected_usage = "qA key=\\\\'\\\\' interval=\[.*\] queries=0 errors=0 result_rows=0 result_bytes=0 read_rows=0 read_bytes=0 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))

    instance.query("SELECT * from test_table")
    expected_usage = "qA key=\\\\'\\\\' interval=\[.*\] queries=1 errors=0 result_rows=50 result_bytes=200 read_rows=50 read_bytes=200 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))

    instance.query("ALTER QUOTA qA RENAME TO qB")
    assert instance.query("SHOW CREATE QUOTA qB") == "CREATE QUOTA qB KEYED BY \\'none\\' FOR RANDOMIZED INTERVAL 16 MONTH TRACKING TO default\n"
    expected_usage = "qB key=\\\\'\\\\' interval=\[.*\] queries=1 errors=0 result_rows=50 result_bytes=200 read_rows=50 read_bytes=200 execution_time=.*"
    assert re.match(expected_usage, instance.query("SHOW QUOTA USAGE"))

    instance.query("DROP QUOTA qB")
    assert instance.query("SHOW QUOTAS") == ""
    assert instance.query("SHOW QUOTA USAGE") == ""


def test_users_xml_is_readonly():
    assert re.search("storage is readonly", instance.query_and_get_error("DROP QUOTA myQuota"))
