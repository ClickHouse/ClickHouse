-- Tags: no-parallel-replicas
-- Reading a subcolumn through a subquery must read only the subcolumn, not the whole column.
-- The check compares the amount of data read by the same query with the optimization off and on.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_push_subcolumns_io;

CREATE TABLE t_push_subcolumns_io (id UInt32, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;

INSERT INTO t_push_subcolumns_io SELECT number, (number, randomPrintableASCII(200)) FROM numbers(100000);

SELECT sum(tup.a) FROM (SELECT * FROM t_push_subcolumns_io)
SETTINGS optimize_push_subcolumns_into_subqueries = 0, log_comment = '04668_off' FORMAT Null;

SELECT sum(tup.a) FROM (SELECT * FROM t_push_subcolumns_io)
SETTINGS optimize_push_subcolumns_into_subqueries = 1, log_comment = '04668_on' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    'read_bytes reduced at least 10 times',
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND log_comment = '04668_on' AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) * 10
    < (SELECT read_bytes FROM system.query_log
       WHERE current_database = currentDatabase() AND log_comment = '04668_off' AND type = 'QueryFinish'
       ORDER BY event_time_microseconds DESC LIMIT 1);

DROP TABLE t_push_subcolumns_io;
