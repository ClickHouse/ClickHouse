set log_queries=1;
set log_queries_min_type='QUERY_FINISH';

DROP TABLE IF EXISTS 02751_query_log_test_partitions;
CREATE TABLE 02751_query_log_test_partitions (a Int64, b Int64) ENGINE = MergeTree PARTITION BY a ORDER BY b;

INSERT INTO 02751_query_log_test_partitions SELECT number, number FROM numbers(10);

SELECT * FROM 02751_query_log_test_partitions WHERE a = 3;

SYSTEM FLUSH LOGS query_log;

SELECT
    --Remove the prefix string which is a mutable database name.
    arrayStringConcat(arrayPopFront(splitByString('.', partitions[1])), '.')
FROM
    system.query_log
WHERE
    current_database=currentDatabase() and
    query = 'SELECT * FROM 02751_query_log_test_partitions WHERE a = 3;'
