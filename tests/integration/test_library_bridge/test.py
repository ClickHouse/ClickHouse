import os
import os.path as p
import pytest
import time

from helpers.cluster import ClickHouseCluster, run_and_check

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance('instance',
        main_configs=[
            'configs/enable_dict.xml',
            'configs/config.d/config.xml',
            'configs/dictionaries/dict1.xml',
            'configs/log_conf.xml'])

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

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    yield  # run test


def test_load_all(ch_cluster):
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
    result = instance.query('''select dictGet(lib_dict_c, 'value1', toUInt64(1));''')
    assert(result.strip() == '101')
    instance.query('DROP DICTIONARY lib_dict_c')


def test_load_keys(ch_cluster):
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
    num_rows = [1000, 10000, 100000, 1000000]
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
    instance.query('SYSTEM RELOAD DICTIONARY dict2')
    instance.query("""
        CREATE TABLE IF NOT EXISTS `dict2_table` (
             key UInt64, value1 UInt64, value2 UInt64, value3 UInt64
        ) ENGINE = Dictionary(dict2)
        """)

    result = instance.query('SELECT * FROM dict2_table ORDER BY key')
    expected = "0\t12\t12\t12\n"
    assert(result == expected)


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
