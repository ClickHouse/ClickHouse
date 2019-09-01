import pytest
import os
import time

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry
from generate_dictionaries import generate_structure, generate_dictionaries, DictionaryTestTable

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = None
instance = None
test_table = None


def get_status(dictionary_name):
    return instance.query("SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'").rstrip("\n")


def get_last_exception(dictionary_name):
    return instance.query("SELECT last_exception FROM system.dictionaries WHERE name='" + dictionary_name + "'").rstrip("\n").replace("\\'", "'")


def get_loading_start_time(dictionary_name):
    s = instance.query("SELECT loading_start_time FROM system.dictionaries WHERE name='" + dictionary_name + "'").rstrip("\n")
    if s == "0000-00-00 00:00:00":
        return None
    return time.strptime(s, "%Y-%m-%d %H:%M:%S")


def get_loading_duration(dictionary_name):
    return float(instance.query("SELECT loading_duration FROM system.dictionaries WHERE name='" + dictionary_name + "'"))


def replace_in_file_in_container(file_name, what, replace_with):
    instance.exec_in_container('sed -i "s/' + what + '/' + replace_with + '/g" ' + file_name)


def setup_module(module):
    global cluster
    global instance
    global test_table

    structure = generate_structure()
    dictionary_files = generate_dictionaries(os.path.join(SCRIPT_DIR, 'configs/dictionaries'), structure)

    cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))
    instance = cluster.add_instance('instance', main_configs=dictionary_files)
    test_table = DictionaryTestTable(os.path.join(SCRIPT_DIR, 'configs/dictionaries/source.tsv'))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance.query("CREATE DATABASE IF NOT EXISTS dict ENGINE=Dictionary")
        test_table.create_clickhouse_source(instance)
        for line in TSV(instance.query('select name from system.dictionaries')).lines:
            print line,

        # Create table `test.small_dict_source`
        instance.query('''
            drop table if exists test.small_dict_source;
            create table test.small_dict_source (id UInt64, a String, b Int32, c Float64) engine=Log;
            insert into test.small_dict_source values (0, 'water', 10, 1), (1, 'air', 40, 0.01), (2, 'earth', 100, 1.7);
            ''')

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(params=[
    # name, keys, use_parent
    ('clickhouse_hashed', ('id',), True),
    ('clickhouse_flat', ('id',), True),
    ('clickhouse_complex_integers_key_hashed', ('key0', 'key1'), False),
    ('clickhouse_complex_mixed_key_hashed', ('key0_str', 'key1'), False),
    ('clickhouse_range_hashed', ('id', 'StartDate', 'EndDate'), False),
],
    ids=['clickhouse_hashed', 'clickhouse_flat',
         'clickhouse_complex_integers_key_hashed',
         'clickhouse_complex_mixed_key_hashed',
         'clickhouse_range_hashed']
)
def dictionary_structure(started_cluster, request):
    return request.param


def test_select_all(dictionary_structure):
    name, keys, use_parent = dictionary_structure
    query = instance.query

    structure = test_table.get_structure_for_keys(keys, use_parent)
    query('''
    DROP TABLE IF EXISTS test.{0}
    '''.format(name))

    create_query = "CREATE TABLE test.{0} ({1}) engine = Dictionary({0})".format(name, structure)
    TSV(query(create_query))

    result = TSV(query('select * from test.{0}'.format(name)))

    diff = test_table.compare_by_keys(keys, result.lines, use_parent, add_not_found_rows=True)
    print test_table.process_diff(diff)
    assert not diff


@pytest.fixture(params=[
    # name, keys, use_parent
    ('clickhouse_cache', ('id',), True),
    ('clickhouse_complex_integers_key_cache', ('key0', 'key1'), False),
    ('clickhouse_complex_mixed_key_cache', ('key0_str', 'key1'), False)
],
    ids=['clickhouse_cache', 'clickhouse_complex_integers_key_cache', 'clickhouse_complex_mixed_key_cache']
)
def cached_dictionary_structure(started_cluster, request):
    return request.param


def test_select_all_from_cached(cached_dictionary_structure):
    name, keys, use_parent = cached_dictionary_structure
    query = instance.query

    structure = test_table.get_structure_for_keys(keys, use_parent)
    query('''
    DROP TABLE IF EXISTS test.{0}
    '''.format(name))

    create_query = "CREATE TABLE test.{0} ({1}) engine = Dictionary({0})".format(name, structure)
    TSV(query(create_query))

    for i in range(4):
        result = TSV(query('select * from test.{0}'.format(name)))
        diff = test_table.compare_by_keys(keys, result.lines, use_parent, add_not_found_rows=False)
        print test_table.process_diff(diff)
        assert not diff

        key = []
        for key_name in keys:
            if key_name.endswith('str'):
                key.append("'" + str(i) + "'")
            else:
                key.append(str(i))
        if len(key) == 1:
            key = 'toUInt64(' + str(i) + ')'
        else:
            key = str('(' + ','.join(key) + ')')
        query("select dictGetUInt8('{0}', 'UInt8_', {1})".format(name, key))

    result = TSV(query('select * from test.{0}'.format(name)))
    diff = test_table.compare_by_keys(keys, result.lines, use_parent, add_not_found_rows=True)
    print test_table.process_diff(diff)
    assert not diff


def test_null_value(started_cluster):
    query = instance.query

    assert TSV(query("select dictGetUInt8('clickhouse_cache', 'UInt8_', toUInt64(12121212))")) == TSV("1")
    assert TSV(query("select dictGetString('clickhouse_cache', 'String_', toUInt64(12121212))")) == TSV("implicit-default")
    assert TSV(query("select dictGetDate('clickhouse_cache', 'Date_', toUInt64(12121212))")) == TSV("2015-11-25")

    # Check, that empty null_value interprets as default value
    assert TSV(query("select dictGetUInt64('clickhouse_cache', 'UInt64_', toUInt64(12121212))")) == TSV("0")
    assert TSV(query("select dictGetDateTime('clickhouse_cache', 'DateTime_', toUInt64(12121212))")) == TSV("0000-00-00 00:00:00")


def test_dictionary_dependency(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so these dictionary are not loaded.
    assert get_status('dep_x') == 'NOT_LOADED'
    assert get_status('dep_y') == 'NOT_LOADED'
    assert get_status('dep_z') == 'NOT_LOADED'

    # Dictionary 'dep_x' depends on 'dep_z', which depends on 'dep_y'.
    # So they all should be loaded at once.
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(1))") == "air\n"
    assert get_status('dep_x') == 'LOADED'
    assert get_status('dep_y') == 'LOADED'
    assert get_status('dep_z') == 'LOADED'

    # Other dictionaries should work too.
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(1))") == "air\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(1))") == "air\n"

    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "YY\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "ZZ\n"

    # Update the source table.
    query("insert into test.small_dict_source values (3, 'fire', 30, 8)")

    # Wait for dictionaries to be reloaded.
    assert_eq_with_retry(instance, "SELECT dictHas('dep_y', toUInt64(3))", "1", sleep_time = 2, retry_count = 10)
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "XX\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "ZZ\n"

    # dep_x and dep_z are updated only when there `intDiv(count(), 4)`  is changed.
    query("insert into test.small_dict_source values (4, 'ether', 404, 0.001)")
    assert_eq_with_retry(instance, "SELECT dictHas('dep_x', toUInt64(4))", "1", sleep_time = 2, retry_count = 10)
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(3))") == "fire\n"
    assert query("SELECT dictGetString('dep_x', 'a', toUInt64(4))") == "ether\n"
    assert query("SELECT dictGetString('dep_y', 'a', toUInt64(4))") == "ether\n"
    assert query("SELECT dictGetString('dep_z', 'a', toUInt64(4))") == "ether\n"


def test_reload_while_loading(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so this dictionary is not loaded.
    assert get_status('longload') == "NOT_LOADED"
    assert get_loading_duration('longload') == 0

    # It's not possible to get a value from the dictionary within 1.0 second, so the following query fails by timeout.
    assert query("SELECT dictGetInt32('longload', 'a', toUInt64(5))", timeout = 1, ignore_error = True) == ""

    # The dictionary is now loading.
    assert get_status('longload') == "LOADING"
    start_time, duration = get_loading_start_time('longload'), get_loading_duration('longload')
    assert duration > 0

    time.sleep(0.5) # Still loading.
    assert get_status('longload') == "LOADING"
    prev_start_time, prev_duration = start_time, duration
    start_time, duration = get_loading_start_time('longload'), get_loading_duration('longload')
    assert start_time == prev_start_time
    assert duration >= prev_duration

    # SYSTEM RELOAD DICTIONARY should restart loading.
    query("SYSTEM RELOAD DICTIONARY 'longload'")
    assert get_status('longload') == "LOADING"
    prev_start_time, prev_duration = start_time, duration
    start_time, duration = get_loading_start_time('longload'), get_loading_duration('longload')
    assert start_time > prev_start_time
    assert duration < prev_duration

    time.sleep(0.5) # Still loading.
    assert get_status('longload') == "LOADING"
    prev_start_time, prev_duration = start_time, duration
    start_time, duration = get_loading_start_time('longload'), get_loading_duration('longload')
    assert start_time == prev_start_time
    assert duration >= prev_duration

    # SYSTEM RELOAD DICTIONARIES should restart loading again.
    query("SYSTEM RELOAD DICTIONARIES")
    assert get_status('longload') == "LOADING"
    prev_start_time, prev_duration = start_time, duration
    start_time, duration = get_loading_start_time('longload'), get_loading_duration('longload')
    assert start_time > prev_start_time
    assert duration < prev_duration

    # Changing the configuration file should restart loading one more time.
    replace_in_file_in_container('/etc/clickhouse-server/config.d/dictionary_preset_longload.xml', 'sleep 100', 'sleep 0')
    time.sleep(5) # Configuration files are reloaded once in 5 seconds.

    # This time loading should finish quickly.
    assert get_status('longload') == "LOADED"
    assert query("SELECT dictGetInt32('longload', 'a', toUInt64(5))") == "6\n"


def test_reload_after_loading(started_cluster):
    query = instance.query

    assert query("SELECT dictGetInt32('cmd', 'a', toUInt64(7))") == "8\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "10\n"

    # Change the dictionaries' data.
    replace_in_file_in_container('/etc/clickhouse-server/config.d/dictionary_preset_cmd.xml', '8', '81')
    replace_in_file_in_container('/etc/clickhouse-server/config.d/dictionary_preset_file.txt', '10', '101')

    # SYSTEM RELOAD 'name' reloads only the specified dictionary.
    query("SYSTEM RELOAD DICTIONARY 'cmd'")
    assert query("SELECT dictGetInt32('cmd', 'a', toUInt64(7))") == "81\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "10\n"

    query("SYSTEM RELOAD DICTIONARY 'file'")
    assert query("SELECT dictGetInt32('cmd', 'a', toUInt64(7))") == "81\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "101\n"

    # SYSTEM RELOAD DICTIONARIES reloads all loaded dictionaries.
    replace_in_file_in_container('/etc/clickhouse-server/config.d/dictionary_preset_cmd.xml', '81', '82')
    replace_in_file_in_container('/etc/clickhouse-server/config.d/dictionary_preset_file.txt', '101', '102')
    query("SYSTEM RELOAD DICTIONARIES")
    assert query("SELECT dictGetInt32('cmd', 'a', toUInt64(7))") == "82\n"
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "102\n"

    # Configuration files are reloaded and lifetimes are checked automatically once in 5 seconds.
    replace_in_file_in_container('/etc/clickhouse-server/config.d/dictionary_preset_cmd.xml', '82', '83')
    replace_in_file_in_container('/etc/clickhouse-server/config.d/dictionary_preset_file.txt', '102', '103')
    time.sleep(5)
    assert query("SELECT dictGetInt32('file', 'a', toUInt64(9))") == "103\n"
    assert query("SELECT dictGetInt32('cmd', 'a', toUInt64(7))") == "83\n"


def test_reload_after_fail_by_system_reload(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so this dictionary is not loaded.
    assert get_status("no_file") == "NOT_LOADED"

    # We expect an error because the file source doesn't exist.
    expected_error = "No such file"
    assert expected_error in instance.query_and_get_error("SELECT dictGetInt32('no_file', 'a', toUInt64(9))")
    assert get_status("no_file") == "FAILED"

    # SYSTEM RELOAD should not change anything now, the status is still FAILED.
    query("SYSTEM RELOAD DICTIONARY 'no_file'")
    assert expected_error in instance.query_and_get_error("SELECT dictGetInt32('no_file', 'a', toUInt64(9))")
    assert get_status("no_file") == "FAILED"

    # Creating the file source makes the dictionary able to load.
    instance.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/dictionaries/dictionary_preset_file.txt"), "/etc/clickhouse-server/config.d/dictionary_preset_no_file.txt")
    query("SYSTEM RELOAD DICTIONARY 'no_file'")
    query("SELECT dictGetInt32('no_file', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file") == "LOADED"

    # Removing the file source should not spoil the loaded dictionary.
    instance.exec_in_container("rm /etc/clickhouse-server/config.d/dictionary_preset_no_file.txt")
    query("SYSTEM RELOAD DICTIONARY 'no_file'")
    query("SELECT dictGetInt32('no_file', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file") == "LOADED"


def test_reload_after_fail_by_timer(started_cluster):
    query = instance.query

    # dictionaries_lazy_load == false, so this dictionary is not loaded.
    assert get_status("no_file_2") == "NOT_LOADED"

    # We expect an error because the file source doesn't exist.
    expected_error = "No such file"
    assert expected_error in instance.query_and_get_error("SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))")
    assert get_status("no_file_2") == "FAILED"

    # Passed time should not change anything now, the status is still FAILED.
    time.sleep(6);
    assert expected_error in instance.query_and_get_error("SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))")
    assert get_status("no_file_2") == "FAILED"

    # Creating the file source makes the dictionary able to load.
    instance.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/dictionaries/dictionary_preset_file.txt"), "/etc/clickhouse-server/config.d/dictionary_preset_no_file_2.txt")
    time.sleep(6);
    query("SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file_2") == "LOADED"

    # Removing the file source should not spoil the loaded dictionary.
    instance.exec_in_container("rm /etc/clickhouse-server/config.d/dictionary_preset_no_file_2.txt")
    time.sleep(6);
    query("SELECT dictGetInt32('no_file_2', 'a', toUInt64(9))") == "10\n"
    assert get_status("no_file_2") == "LOADED"


def test_reload_after_fail_in_cache_dictionary(started_cluster):
    query = instance.query
    query_and_get_error = instance.query_and_get_error

    # Can't get a value from the cache dictionary because the source (table `test.xypairs`) doesn't respond.
    expected_error = "Table test.xypairs doesn't exist"
    assert expected_error in query_and_get_error("SELECT dictGetUInt64('cache_xypairs', 'y', toUInt64(1))")
    assert get_status("cache_xypairs") == "LOADED"
    assert expected_error in get_last_exception("cache_xypairs")

    # Create table `test.xypairs`.
    query('''
        drop table if exists test.xypairs;
        create table test.xypairs (x UInt64, y UInt64) engine=Log;
        insert into test.xypairs values (1, 56), (3, 78);
        ''')

    # Cache dictionary now works.
    assert_eq_with_retry(instance, "SELECT dictGet('cache_xypairs', 'y', toUInt64(1))", "56", ignore_error=True)
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    assert get_last_exception("cache_xypairs") == ""

    # Drop table `test.xypairs`.
    query('drop table if exists test.xypairs')

    # Values are cached so we can get them.
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(1))") == "56"
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    assert get_last_exception("cache_xypairs") == ""

    # But we can't get a value from the source table which isn't cached.
    assert expected_error in query_and_get_error("SELECT dictGetUInt64('cache_xypairs', 'y', toUInt64(3))")
    assert expected_error in get_last_exception("cache_xypairs")

    # Passed time should not spoil the cache.
    time.sleep(5);
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(1))") == "56"
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    assert expected_error in query_and_get_error("SELECT dictGetUInt64('cache_xypairs', 'y', toUInt64(3))")
    assert expected_error in get_last_exception("cache_xypairs")

    # Create table `test.xypairs` again with changed values.
    query('''
        drop table if exists test.xypairs;
        create table test.xypairs (x UInt64, y UInt64) engine=Log;
        insert into test.xypairs values (1, 57), (3, 79);
        ''')

    # The cache dictionary returns new values now.
    assert_eq_with_retry(instance, "SELECT dictGet('cache_xypairs', 'y', toUInt64(1))", "57")
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(2))") == "0"
    query("SELECT dictGet('cache_xypairs', 'y', toUInt64(3))") == "79"
    assert get_last_exception("cache_xypairs") == ""
