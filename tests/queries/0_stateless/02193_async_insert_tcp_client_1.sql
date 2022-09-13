DROP TABLE IF EXISTS t_async_insert_02193_1;

CREATE TABLE t_async_insert_02193_1 (id UInt32, s String) ENGINE = Memory;

INSERT INTO t_async_insert_02193_1 FORMAT CSV SETTINGS async_insert = 1
1,aaa
;

INSERT INTO t_async_insert_02193_1 FORMAT Values SETTINGS async_insert = 1 (2, 'bbb');

SET async_insert = 1;

INSERT INTO t_async_insert_02193_1 VALUES (3, 'ccc');
INSERT INTO t_async_insert_02193_1 FORMAT JSONEachRow {"id": 4, "s": "ddd"};

SELECT * FROM t_async_insert_02193_1 ORDER BY id;

SYSTEM FLUSH LOGS;

SELECT count(), sum(ProfileEvents['AsyncInsertQuery']) FROM system.query_log
WHERE
    event_date >= yesterday() AND
    type = 'QueryFinish' AND
    current_database = currentDatabase() AND
    query ILIKE 'INSERT INTO t_async_insert_02193_1%';

DROP TABLE IF EXISTS t_async_insert_02193_1;
