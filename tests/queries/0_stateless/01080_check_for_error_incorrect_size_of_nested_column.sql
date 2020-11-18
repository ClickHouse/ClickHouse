drop table if exists test_table_01080;
CREATE TABLE test_table_01080 (dim_key Int64, dim_id String) ENGINE = MergeTree Order by (dim_key);
insert into test_table_01080 values(1,'test1');

drop DICTIONARY if exists test_dict_01080;

CREATE DICTIONARY test_dict_01080 ( dim_key Int64, dim_id String )
PRIMARY KEY dim_key
source(clickhouse(host 'localhost' port tcpPort() user 'default' password '' db currentDatabase() table 'test_table_01080'))
LIFETIME(MIN 0 MAX 0) LAYOUT(complex_key_hashed());

SELECT dictGetString('test_dict_01080', 'dim_id', tuple(toInt64(1)));

SELECT dictGetString('test_dict_01080', 'dim_id', tuple(toInt64(0)));

select dictGetString('test_dict_01080', 'dim_id', x)  from (select tuple(toInt64(0)) as x);

select dictGetString('test_dict_01080', 'dim_id', x)  from (select tuple(toInt64(1)) as x);

select dictGetString('test_dict_01080', 'dim_id', x)  from (select tuple(toInt64(number)) as x from numbers(5));

select dictGetString('test_dict_01080', 'dim_id', x) from (select tuple(toInt64(rand64()*0)) as x);

select dictGetString('test_dict_01080', 'dim_id', x) from (select tuple(toInt64(blockSize()=0)) as x);

select dictGetString('test_dict_01080', 'dim_id', x) from (select tuple(toInt64(materialize(0))) as x);

select dictGetString('test_dict_01080', 'dim_id', x) from (select tuple(toInt64(materialize(1))) as x);


drop DICTIONARY   test_dict_01080;
drop table   test_table_01080;
