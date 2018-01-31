import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance('node', main_configs=['configs/mergetree_settings.xml'])



@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):

    instance.query('create database if not exists test;')

    instance.query('''
    drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value String, version UInt64, sign Int8) engine = MultiversionMergeTree(date, date, 8192, sign, version);
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
    ''')

    assert instance.query("select count() from test.mult_tab final;") == '0\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '2\n'


    instance.query('''
drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value String, version UInt64, sign Int8) engine = MultiversionMergeTree(date, (date, value), 8192, sign, version);
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
        ''')

    assert instance.query("select count() from test.mult_tab final;") == '20\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '20\n'


    instance.query('''
drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value String, version UInt64, sign Int8) engine = MultiversionMergeTree(date, (date, value), 8192, sign, version);
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, -1, 1) from system.numbers limit 10;
            ''')

    assert instance.query("select count() from test.mult_tab final;") == '0\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '2\n'


    instance.query('''
drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value String, version UInt64, sign Int8) engine = MultiversionMergeTree(date, (date, value), 8192, sign, version);
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 1, if(number % 2, -1, 1) from system.numbers limit 10;
                ''')

    assert instance.query("select count() from test.mult_tab final;") == '20\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '20\n'


    instance.query('''
drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value String, version UInt64, sign Int8) engine = MultiversionMergeTree(date, (date, value), 8192, sign, version);
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, -1, 1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, -1, 1) from system.numbers limit 10;
                    ''')

    assert instance.query("select count() from test.mult_tab final;") == '0\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '2\n'


    instance.query('''
drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value String, version UInt64, sign Int8) engine = MultiversionMergeTree(date, (date, value), 8192, sign, version);
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, -1, 1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 1, if(number % 3 = 0, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 1, if(number % 3 = 1, 1, -1) from system.numbers limit 10;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 1, if(number % 3 = 2, 1, -1) from system.numbers limit 10;
                        ''')

    assert instance.query("select count() from test.mult_tab final;") == '10\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '10\n'


    instance.query('''
drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value String, version UInt64, sign Int8) engine = MultiversionMergeTree(date, (date, value), 8192, sign, version);
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, 1, -1) from system.numbers limit 1000000;
insert into test.mult_tab select '2018-01-31', 'str_' || toString(number), 0, if(number % 2, -1, 1) from system.numbers limit 1000000;
                            ''')

    assert instance.query("select count() from test.mult_tab final;") == '0\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '2\n'


    instance.query('''
drop table if exists test.mult_tab;
create table test.mult_tab (date Date, value UInt64, version UInt64, sign Int8) engine = MultiversionMergeTree(date, (date), 8192, sign, version);
insert into test.mult_tab select '2018-01-31', number, 0, if(number < 64, 1, -1) from system.numbers limit 128;
insert into test.mult_tab select '2018-01-31', number, 0, if(number < 64, -1, 1) from system.numbers limit 128;
                            ''')

    assert instance.query("select count() from test.mult_tab final settings max_block_size=32;") == '128\n'
    instance.query('optimize table test.mult_tab;')
    assert instance.query("select count() from test.mult_tab;") == '2\n'
