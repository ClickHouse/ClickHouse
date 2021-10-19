import os
import os.path as p
import pytest
import time
import logging

from helpers.cluster import ClickHouseCluster, run_and_check

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance('instance',
        dictionaries=['configs/dictionaries/dict1.xml'], main_configs=['configs/config.d/config.xml'], stay_alive=True)


def create_dict_simple():
    instance.query('DROP DICTIONARY IF EXISTS lib_dict_c')
    instance.query('''
        CREATE DICTIONARY lib_dict_c (key UInt64, value1 UInt64, value2 UInt64, value3 UInt64)
        PRIMARY KEY key SOURCE(library(PATH '/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.so'))
        LAYOUT(CACHE(
        SIZE_IN_CELLS 10000000
        BLOCK_SIZE 4096
        FILE_SIZE 16777216
        READ_BUFFER_SIZE 1048576
        MAX_STORED_KEYS 1048576))
        LIFETIME(2) ;
    ''')


@pytest.fixture(scope="module")
def ch_cluster():
    try:
        cluster.start()
        instance.query('CREATE DATABASE test')
        container_lib_path = '/etc/clickhouse-server/config.d/dictionarites_lib/dict_lib.cpp'

        instance.copy_file_to_container(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/dict_lib.cpp"),
                                                        "/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.cpp")

        instance.query("SYSTEM RELOAD CONFIG")

        instance.exec_in_container(
                ['bash', '-c',
                '/usr/bin/g++ -shared -o /etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.so -fPIC /etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.cpp'],
                user='root')

        instance.exec_in_container(
                ['bash', '-c',
                '/usr/bin/g++ -shared -o /dict_lib_copy.so -fPIC /etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.cpp'], user='root')
        instance.exec_in_container(['bash', '-c', 'ln -s /dict_lib_copy.so /etc/clickhouse-server/config.d/dictionaries_lib/dict_lib_symlink.so'])

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    yield  # run test


def test_load_all(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query('DROP DICTIONARY IF EXISTS lib_dict')
    instance.query('''
        CREATE DICTIONARY lib_dict (key UInt64, value1 UInt64, value2 UInt64, value3 UInt64)
        PRIMARY KEY key
        SOURCE(library(
            PATH '/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.so'
            SETTINGS (test_type test_simple)))
        LAYOUT(HASHED())
        LIFETIME (MIN 0 MAX 10)
    ''')

    result = instance.query('SELECT * FROM lib_dict ORDER BY key')
    expected = (
"0\t10\t20\t30\n" +
"1\t11\t21\t31\n" +
"2\t12\t22\t32\n" +
"3\t13\t23\t33\n" +
"4\t14\t24\t34\n" +
"5\t15\t25\t35\n" +
"6\t16\t26\t36\n" +
"7\t17\t27\t37\n" +
"8\t18\t28\t38\n" +
"9\t19\t29\t39\n"
)
    instance.query('SYSTEM RELOAD DICTIONARY dict1')
    instance.query('DROP DICTIONARY lib_dict')
    assert(result == expected)

    instance.query("""
        CREATE TABLE IF NOT EXISTS `dict1_table` (
             key UInt64, value1 UInt64, value2 UInt64, value3 UInt64
        ) ENGINE = Dictionary(dict1)
        """)

    result = instance.query('SELECT * FROM dict1_table ORDER BY key')
    assert(result == expected)


def test_load_ids(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query('DROP DICTIONARY IF EXISTS lib_dict_c')
    instance.query('''
        CREATE DICTIONARY lib_dict_c (key UInt64, value1 UInt64, value2 UInt64, value3 UInt64)
        PRIMARY KEY key SOURCE(library(PATH '/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.so'))
        LAYOUT(CACHE(
        SIZE_IN_CELLS 10000000
        BLOCK_SIZE 4096
        FILE_SIZE 16777216
        READ_BUFFER_SIZE 1048576
        MAX_STORED_KEYS 1048576))
        LIFETIME(2) ;
    ''')

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(0));''')
    assert(result.strip() == '100')

    # Just check bridge is ok with a large vector of random ids
    instance.query('''select number, dictGet(lib_dict_c, 'value1', toUInt64(rand())) from numbers(1000);''')

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')
    instance.query('DROP DICTIONARY lib_dict_c')


def test_load_keys(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query('DROP DICTIONARY IF EXISTS lib_dict_ckc')
    instance.query('''
        CREATE DICTIONARY lib_dict_ckc (key UInt64, value1 UInt64, value2 UInt64, value3 UInt64)
        PRIMARY KEY key
        SOURCE(library(PATH '/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.so'))
        LAYOUT(COMPLEX_KEY_CACHE( SIZE_IN_CELLS 10000000))
        LIFETIME(2);
    ''')

    result = instance.query('''select dictGet(lib_dict_ckc, 'value1', tuple(toUInt64(0)));''')
    assert(result.strip() == '100')
    result = instance.query('''select dictGet(lib_dict_ckc, 'value2', tuple(toUInt64(0)));''')
    assert(result.strip() == '200')
    instance.query('DROP DICTIONARY lib_dict_ckc')


def test_load_all_many_rows(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    num_rows = [1000, 10000, 100000, 1000000]
    instance.query('DROP DICTIONARY IF EXISTS lib_dict')
    for num in num_rows:
        instance.query('''
            CREATE DICTIONARY lib_dict (key UInt64, value1 UInt64, value2 UInt64, value3 UInt64)
            PRIMARY KEY key
            SOURCE(library(
                PATH '/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.so'
                SETTINGS (num_rows {} test_type test_many_rows)))
            LAYOUT(HASHED())
            LIFETIME (MIN 0 MAX 10)
        '''.format(num))

        result = instance.query('SELECT * FROM lib_dict ORDER BY key')
        expected = instance.query('SELECT number, number, number, number FROM numbers({})'.format(num))
        instance.query('DROP DICTIONARY lib_dict')
        assert(result == expected)


def test_null_values(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query('SYSTEM RELOAD DICTIONARY dict2')
    instance.query("""
        CREATE TABLE IF NOT EXISTS `dict2_table` (
             key UInt64, value1 UInt64, value2 UInt64, value3 UInt64
        ) ENGINE = Dictionary(dict2)
        """)

    result = instance.query('SELECT * FROM dict2_table ORDER BY key')
    expected = "0\t12\t12\t12\n"
    assert(result == expected)


def test_recover_after_bridge_crash(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    create_dict_simple()

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(0));''')
    assert(result.strip() == '100')
    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')

    instance.exec_in_container(['bash', '-c', 'kill -9 `pidof clickhouse-library-bridge`'], user='root')
    instance.query('SYSTEM RELOAD DICTIONARY lib_dict_c')

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(0));''')
    assert(result.strip() == '100')
    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')

    instance.exec_in_container(['bash', '-c', 'kill -9 `pidof clickhouse-library-bridge`'], user='root')
    instance.query('DROP DICTIONARY lib_dict_c')


def test_server_restart_bridge_might_be_stil_alive(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    create_dict_simple()

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')

    instance.restart_clickhouse()

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')

    instance.exec_in_container(['bash', '-c', 'kill -9 `pidof clickhouse-library-bridge`'], user='root')
    instance.restart_clickhouse()

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')

    instance.query('DROP DICTIONARY lib_dict_c')


def test_bridge_dies_with_parent(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")
    if instance.is_built_with_address_sanitizer():
        pytest.skip("Leak sanitizer falsely reports about a leak of 16 bytes in clickhouse-odbc-bridge")

    create_dict_simple()
    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')

    clickhouse_pid = instance.get_process_pid("clickhouse server")
    bridge_pid = instance.get_process_pid("library-bridge")
    assert clickhouse_pid is not None
    assert bridge_pid is not None

    while clickhouse_pid is not None:
        try:
            instance.exec_in_container(["kill", str(clickhouse_pid)], privileged=True, user='root')
        except:
            pass
        clickhouse_pid = instance.get_process_pid("clickhouse server")
        time.sleep(1)

    for i in range(30):
        time.sleep(1)
        bridge_pid = instance.get_process_pid("library-bridge")
        if bridge_pid is None:
            break

    if bridge_pid:
        out = instance.exec_in_container(["gdb", "-p", str(bridge_pid), "--ex", "thread apply all bt", "--ex", "q"],
                                      privileged=True, user='root')
        logging.debug(f"Bridge is running, gdb output:\n{out}")

    assert clickhouse_pid is None
    assert bridge_pid is None
    instance.start_clickhouse(20)
    instance.query('DROP DICTIONARY lib_dict_c')


def test_path_validation(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    instance.query('DROP DICTIONARY IF EXISTS lib_dict_c')
    instance.query('''
        CREATE DICTIONARY lib_dict_c (key UInt64, value1 UInt64, value2 UInt64, value3 UInt64)
        PRIMARY KEY key SOURCE(library(PATH '/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib_symlink.so'))
        LAYOUT(CACHE(
        SIZE_IN_CELLS 10000000
        BLOCK_SIZE 4096
        FILE_SIZE 16777216
        READ_BUFFER_SIZE 1048576
        MAX_STORED_KEYS 1048576))
        LIFETIME(2) ;
    ''')

    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')

    instance.query('DROP DICTIONARY IF EXISTS lib_dict_c')
    instance.query('''
        CREATE DICTIONARY lib_dict_c (key UInt64, value1 UInt64, value2 UInt64, value3 UInt64)
        PRIMARY KEY key SOURCE(library(PATH '/etc/clickhouse-server/config.d/dictionaries_lib/../../../../dict_lib_copy.so'))
        LAYOUT(CACHE(
        SIZE_IN_CELLS 10000000
        BLOCK_SIZE 4096
        FILE_SIZE 16777216
        READ_BUFFER_SIZE 1048576
        MAX_STORED_KEYS 1048576))
        LIFETIME(2) ;
    ''')
    result = instance.query_and_get_error('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert('DB::Exception: File path /etc/clickhouse-server/config.d/dictionaries_lib/../../../../dict_lib_copy.so is not inside /etc/clickhouse-server/config.d/dictionaries_lib' in result)


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
