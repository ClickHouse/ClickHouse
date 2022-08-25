DROP TABLE IF EXISTS t_async_insert_table_function;

CREATE TABLE t_async_insert_table_function (id UInt32, s String) ENGINE = Memory;

SET async_insert = 1;

INSERT INTO function remote('127.0.0.1', currentDatabase(), t_async_insert_table_function) values (1, 'aaa') (2, 'bbb');

SELECT * FROM t_async_insert_table_function ORDER BY id;

DROP TABLE t_async_insert_table_function;
