-- Tags: no-fasttest
-- no-fasttest since query does cross join, it takes time

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree ORDER BY key;
insert into t1 Select number, number from numbers(100000);
insert into t2 Select number, number from numbers(10000);

SELECT
    1 * 1000.0001,
    (count(1.) = -2147483647) AND (count(a) = 1.1920928955078125e-7) AND (count(val) = 1048577) AND (sum(val) = ((NULL * 1048576) / -9223372036854775807)) AND (sum(a) = ((9223372036854775806 * 10000000000.) / 1048575))
FROM
(
    SELECT
        a,
        val
    FROM t1
    FULL OUTER JOIN t2 ON (t1.a = t2.key) OR 1.0 OR (t1.b = t2.key)
)
GROUP BY '65537'
    WITH CUBE
SETTINGS max_block_size = 100, receive_timeout = 10., receive_data_timeout_ms = 10000, alter_sync = 2, allow_suspicious_low_cardinality_types = 1, log_queries = 1, table_function_remote_max_addresses = 200, join_use_nulls = 1, allow_experimental_analyzer = 1, max_execution_time = 0, max_memory_usage = 10000000000, send_logs_level = 'fatal', allow_introspection_functions = 1, insert_keeper_fault_injection_probability = 0.;

DROP TABLE t1;
DROP TABLE t2;
