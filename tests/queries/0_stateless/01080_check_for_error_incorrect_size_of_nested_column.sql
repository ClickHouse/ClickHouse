-- Tags: no-parallel

-- TODO: can't just remove default prefix, it breaks the test!

drop database if exists db_01080;
create database db_01080;

drop table if exists db_01080.test_table_01080;
CREATE TABLE db_01080.test_table_01080 (dim_key Int64, dim_id String) ENGINE = MergeTree Order by (dim_key);
insert into db_01080.test_table_01080 values(1,'test1');

drop DICTIONARY if exists db_01080.test_dict_01080;

CREATE DICTIONARY db_01080.test_dict_01080 ( dim_key Int64, dim_id String )
PRIMARY KEY dim_key
source(clickhouse(host 'localhost' port tcpPort() user 'default' password '' db 'db_01080' table 'test_table_01080'))
LIFETIME(MIN 0 MAX 0) LAYOUT(complex_key_hashed());

SELECT dictGetString('db_01080.test_dict_01080', 'dim_id', tuple(toInt64(1)));

SELECT dictGetString('db_01080.test_dict_01080', 'dim_id', tuple(toInt64(0)));

select dictGetString('db_01080.test_dict_01080', 'dim_id', x)  from (select tuple(toInt64(0)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x)  from (select tuple(toInt64(1)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x)  from (select tuple(toInt64(number)) as x from numbers(5));

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(toInt64(rand64()*0)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(toInt64(blockSize()=0)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(toInt64(materialize(0))) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(toInt64(materialize(1))) as x);


drop DICTIONARY   db_01080.test_dict_01080;
drop table   db_01080.test_table_01080;
drop database db_01080;
