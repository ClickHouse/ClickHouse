import pytest
import os
import time

## sudo -H pip install PyMySQL
import pymysql.cursors

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DICTIONARY_FILES = ['configs/dictionaries/mysql_dict1.xml', 'configs/dictionaries/mysql_dict2.xml']

cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))
instance = cluster.add_instance('instance', main_configs=DICTIONARY_FILES, with_mysql = True)

create_table_mysql_template = """
    CREATE TABLE `test`.`{}` (
        `id` int(11) NOT NULL,
        `value` varchar(50) NOT NULL,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB;
    """

create_clickhouse_dictionary_table_template = """
    CREATE TABLE `test`.`dict_table_{}` (`id` Int32, `value` String) ENGINE = Dictionary({})
    ORDER BY `id` DESC SETTINGS index_granularity = 8192
    """

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        
        # Create a MySQL database
        create_mysql_db(get_mysql_conn(), 'test')
        
        # Create database in ClickHouse
        instance.query("CREATE DATABASE IF NOT EXISTS test")
    
        # Create database in ClickChouse using MySQL protocol (will be used for data insertion)
        instance.query("CREATE DATABASE clickhouse_mysql ENGINE = MySQL('mysql1:3306', 'test', 'root', 'clickhouse')")
        
        yield cluster

    finally:
        cluster.shutdown()


def test_load_mysql_dictionaries(started_cluster):
    # Load dictionaries
    query = instance.query
    query("SYSTEM RELOAD DICTIONARIES")
    
    for n in range(0, 5):
        # Create MySQL tables, fill them and create CH dict tables
        prepare_tables('test' + n)
    
    # Check dictionaries are loaded and have correct number of elements
    for n in range(0, 100):
        # Force reload of dictionaries (each 10 iteration)
        if (n % 10) == 0:
            query("SYSTEM RELOAD DICTIONARIES")

        # Check number of rows            
        assert query("SELECT count() FROM `dict_table_`.{}".format('test' + (n % 5))).rstrip() == '10000'

def create_mysql_db(mysql_connection, name):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))

def prepare_mysql_table(table_name):
    mysql_connection = get_mysql_conn()
    
    # Create table
    create_mysql_table(mysql_connection, table_name)

    # Insert rows using CH
    query = instance.query
    query("INSERT INTO `clickhouse_mysql`.{}(id, value) select number, concat('{} value ', toString(number)) from numbers(10000) ".format(table_name, table_name))
    assert query("SELECT count() FROM `clickhouse_mysql`.{}".format(table_name)).rstrip() == '10000'
    mysql_connection.close()
    
    #Create CH Dictionary tables based on MySQL tables 
    query(create_clickhouse_dictionary_table_template.format(table_name + n), 'dict' + n)

def get_mysql_conn():
    conn = pymysql.connect(user='root', password='clickhouse', host='mysql1', port=3308)
    return conn

def create_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(create_table_mysql_template.format(table_name))

