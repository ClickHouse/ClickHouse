import os
import time

import pytest
from helpers.client import QueryTimeoutExceedException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DICTIONARY_FILES = [
    "configs/dictionaries/cache_xypairs.xml",
    "configs/dictionaries/executable.xml",
    "configs/dictionaries/file.xml",
    "configs/dictionaries/file.txt",
    "configs/dictionaries/slow.xml",
]

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", dictionaries=DICTIONARY_FILES)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query("CREATE DATABASE IF NOT EXISTS test")

        yield cluster

    finally:
        cluster.shutdown()


def get_status(dictionary_name):
    return instance.query(
        "SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'"
    ).rstrip("\n")


def get_last_exception(dictionary_name):
    return (
        instance.query(
            "SELECT last_exception FROM system.dictionaries WHERE name='"
            + dictionary_name
            + "'"
        )
        .rstrip("\n")
        .replace("\\'", "'")
    )


def get_loading_start_time(dictionary_name):
    s = instance.query(
        "SELECT toTimeZone(loading_start_time, 'UTC') FROM system.dictionaries WHERE name='"
        + dictionary_name
        + "'"
    ).rstrip("\n")
    if s == "1970-01-01 00:00:00":
        return None
    return time.strptime(s, "%Y-%m-%d %H:%M:%S")


def get_last_successful_update_time(dictionary_name):
    s = instance.query(
        "SELECT toTimeZone(last_successful_update_time, 'UTC') FROM system.dictionaries WHERE name='"
        + dictionary_name
        + "'"
    ).rstrip("\n")
    if s == "1970-01-01 00:00:00":
        return None
    return time.strptime(s, "%Y-%m-%d %H:%M:%S")


def get_loading_duration(dictionary_name):
    return float(
        instance.query(
            "SELECT loading_duration FROM system.dictionaries WHERE name='"
            + dictionary_name
            + "'"
        )
    )


def replace_in_file_in_container(file_name, what, replace_with):
    instance.exec_in_container(["sed", "-i", f"s/{what}/{replace_with}/g", file_name])


def test_reload_while_loading(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so this dictionary is not loaded.
    assert get_status("slow") == "NOT_LOADED"
    assert get_loading_duration("slow") == 0

    # It's not possible to get a value from the dictionary within 1 second, so the following query fails by timeout.
    with pytest.raises(QueryTimeoutExceedException):
        query("SELECT dictGetInt32('slow', 'a', toUInt64(5))", timeout=1)

    # The dictionary is now loading.
    assert get_status("slow") == "LOADING"
    start_time, duration = get_loading_start_time("slow"), get_loading_duration("slow")
    assert duration > 0

    time.sleep(1)  # Still loading.
    assert get_status("slow") == "LOADING"
    prev_start_time, prev_duration = start_time, duration
    start_time, duration = get_loading_start_time("slow"), get_loading_duration("slow")
    assert start_time == prev_start_time
    assert duration >= prev_duration

    # SYSTEM RELOAD DICTIONARY should restart loading.
    with pytest.raises(QueryTimeoutExceedException):
        query("SYSTEM RELOAD DICTIONARY 'slow'", timeout=1)
    assert get_status("slow") == "LOADING"
    prev_start_time, prev_duration = start_time, duration
    start_time, duration = get_loading_start_time("slow"), get_loading_duration("slow")
    assert start_time > prev_start_time
    assert duration < prev_duration

    time.sleep(1)  # Still loading.
    assert get_status("slow") == "LOADING"
    prev_start_time, prev_duration = start_time, duration
    start_time, duration = get_loading_start_time("slow"), get_loading_duration("slow")
    assert start_time == prev_start_time
    assert duration >= prev_duration

    # Changing the configuration file should restart loading again.
    replace_in_file_in_container(
        "/etc/clickhouse-server/dictionaries/slow.xml", "sleep 100", "sleep 0"
    )
    query("SYSTEM RELOAD CONFIG")

    # This time loading should finish quickly.
    assert get_status("slow") == "LOADED"

    last_successful_update_time = get_last_successful_update_time("slow")
    assert last_successful_update_time > start_time
    assert query("SELECT dictGetInt32('slow', 'a', toUInt64(5))") == "6\n"


def test_reload_after_loading(started_cluster):
    query = instance.query

    assert query("SELECT dictGetInt32('executable', 'a', toUInt64(7))") == "8\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "10\n"

    # Change the dictionaries' data.
    # FIXME we sleep before this, because Poco 1.x has one-second granularity
    # for mtime, and clickhouse will miss the update if we change the file too
    # soon. Should probably be fixed by switching to use std::filesystem.
    time.sleep(1)
    replace_in_file_in_container(
        "/etc/clickhouse-server/dictionaries/executable.xml", "8", "81"
    )
    replace_in_file_in_container(
        "/etc/clickhouse-server/dictionaries/file.txt", "10", "101"
    )

    # SYSTEM RELOAD 'name' reloads only the specified dictionary.
    query("SYSTEM RELOAD DICTIONARY 'executable'")
    assert query("SELECT dictGetInt32('executable', 'a', toUInt64(7))") == "81\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "10\n"

    query("SYSTEM RELOAD DICTIONARY 'file'")
    assert query("SELECT dictGetInt32('executable', 'a', toUInt64(7))") == "81\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "101\n"

    # SYSTEM RELOAD DICTIONARIES reloads all loaded dictionaries.
    time.sleep(1)  # see the comment above
    replace_in_file_in_container(
        "/etc/clickhouse-server/dictionaries/executable.xml", "81", "82"
    )
    replace_in_file_in_container(
        "/etc/clickhouse-server/dictionaries/file.txt", "101", "102"
    )
    query("SYSTEM RELOAD DICTIONARY 'file'")
    query("SYSTEM RELOAD DICTIONARY 'executable'")
    assert query("SELECT dictGetInt32('executable', 'a', toUInt64(7))") == "82\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "102\n"

    # Configuration files are reloaded and lifetimes are checked automatically once in 5 seconds.
    # Wait slightly more, to be sure it did reload.
    time.sleep(1)  # see the comment above
    replace_in_file_in_container(
        "/etc/clickhouse-server/dictionaries/executable.xml", "82", "83"
    )
    replace_in_file_in_container(
        "/etc/clickhouse-server/dictionaries/file.txt", "102", "103"
    )
    time.sleep(10)
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "103\n"
    assert query("SELECT dictGetInt32('executable', 'a', toUInt64(7))") == "83\n"


def test_reload_after_fail_by_system_reload(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so this dictionary is not loaded.
    assert get_status("no_file") == "NOT_LOADED"

    # We expect an error because the file source doesn't exist.
    no_such_file_error = "No such file"
    assert no_such_file_error in instance.query_and_get_error(
        "SELECT dictGetInt32('no_file', 'a', toUInt64(9))"
    )
    assert get_status("no_file") == "FAILED"

    # SYSTEM RELOAD should not change anything now, the status is still FAILED.
    assert no_such_file_error in instance.query_and_get_error(
        "SYSTEM RELOAD DICTIONARY 'no_file'"
    )
    assert no_such_file_error in instance.query_and_get_error(
        "SELECT dictGetInt32('no_file', 'a', toUInt64(9))"
    )
    assert get_status("no_file") == "FAILED"

    # Creating the file source makes the dictionary able to load.
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/dictionaries/file.txt"),
        "/etc/clickhouse-server/dictionaries/no_file.txt",
    )
    query("SYSTEM RELOAD DICTIONARY 'no_file'")
    query("SELECT dictGetInt32('no_file', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file") == "LOADED"

    # Removing the file source should not spoil the loaded dictionary.
    instance.exec_in_container(
        ["rm", "/etc/clickhouse-server/dictionaries/no_file.txt"]
    )
    assert no_such_file_error in instance.query_and_get_error(
        "SYSTEM RELOAD DICTIONARY 'no_file'"
    )
    query("SELECT dictGetInt32('no_file', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file") == "LOADED"


def test_reload_after_fail_by_timer(started_cluster):
    # dictionaries_lazy_load == false, so this dictionary is not loaded.
    assert get_status("no_file_2") == "NOT_LOADED"

    # We expect an error because the file source doesn't exist.
    expected_error = "No such file"
    assert expected_error in instance.query_and_get_error(
        "SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))"
    )
    assert get_status("no_file_2") == "FAILED"

    # Passed time should not change anything now, the status is still FAILED.
    time.sleep(6)
    assert expected_error in instance.query_and_get_error(
        "SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))"
    )

    # on sanitizers builds it can return 'FAILED_AND_RELOADING' which is not quite right
    # add retry for these builds
    assert get_status("no_file_2") in ["FAILED", "FAILED_AND_RELOADING"]

    # Creating the file source makes the dictionary able to load.
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/dictionaries/file.txt"),
        "/etc/clickhouse-server/dictionaries/no_file_2.txt",
    )
    # Check that file appears in container and wait if needed.
    while not instance.path_exists("/etc/clickhouse-server/dictionaries/no_file_2.txt"):
        time.sleep(1)
    assert "9\t10\n" == instance.exec_in_container(
        ["cat", "/etc/clickhouse-server/dictionaries/no_file_2.txt"]
    )
    instance.query("SYSTEM RELOAD DICTIONARY no_file_2")
    instance.query("SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file_2") in ["LOADED", "LOADED_AND_RELOADING"]

    # Removing the file source should not spoil the loaded dictionary.
    instance.exec_in_container(
        ["rm", "/etc/clickhouse-server/dictionaries/no_file_2.txt"]
    )
    time.sleep(6)
    instance.query("SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file_2") in ["LOADED", "LOADED_AND_RELOADING"]


def test_reload_after_fail_in_cache_dictionary(started_cluster):
    query = instance.query
    query_and_get_error = instance.query_and_get_error

    # Can't get a value from the cache dictionary because the source (table `test.xypairs`) doesn't respond.
    expected_error = "UNKNOWN_TABLE"
    update_error = "Could not update cache dictionary cache_xypairs now"
    assert expected_error in query_and_get_error(
        "SELECT dictGetUInt64('cache_xypairs', 'y', toUInt64(1))"
    )
    assert get_status("cache_xypairs") == "LOADED"
    assert expected_error in get_last_exception("cache_xypairs")

    # Create table `test.xypairs`.
    query(
        """
        DROP TABLE IF EXISTS test.xypairs;
        CREATE TABLE test.xypairs (x UInt64, y UInt64) ENGINE=Log;
        INSERT INTO test.xypairs VALUES (1, 56), (3, 78);
        """
    )

    # Cache dictionary now works.
    assert_eq_with_retry(
        instance,
        "SELECT dictGet('cache_xypairs', 'y', toUInt64(1))",
        "56",
        ignore_error=True,
    )
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    assert get_last_exception("cache_xypairs") == ""

    # Drop table `test.xypairs`.
    query("DROP TABLE test.xypairs")

    # Values are cached so we can get them.
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(1))") == "56"
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    assert get_last_exception("cache_xypairs") == ""

    # But we can't get a value from the source table which isn't cached.
    assert expected_error in query_and_get_error(
        "SELECT dictGetUInt64('cache_xypairs', 'y', toUInt64(3))"
    )
    assert expected_error in get_last_exception("cache_xypairs")

    # Passed time should not spoil the cache.
    time.sleep(5)
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(1))") == "56"
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    error = query_and_get_error(
        "SELECT dictGetUInt64('cache_xypairs', 'y', toUInt64(3))"
    )
    assert (expected_error in error) or (update_error in error)
    last_exception = get_last_exception("cache_xypairs")
    assert (expected_error in last_exception) or (update_error in last_exception)

    # Create table `test.xypairs` again with changed values.
    query(
        """
        CREATE TABLE test.xypairs (x UInt64, y UInt64) ENGINE=Log;
        INSERT INTO test.xypairs VALUES (1, 57), (3, 79);
        """
    )

    query("SYSTEM RELOAD DICTIONARY cache_xypairs")

    # The cache dictionary returns new values now.
    assert_eq_with_retry(
        instance, "SELECT dictGet('cache_xypairs', 'y', toUInt64(1))", "57"
    )
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(3))") == "79"
    assert get_last_exception("cache_xypairs") == ""
