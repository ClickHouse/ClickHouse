-- Tags: no-fasttest, no-ubsan, no-cpu-aarch64

SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SYSTEM DROP COMPILED EXPRESSION CACHE;

SELECT number + number + number FROM numbers(1);

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['CompileFunction'] FROM system.query_log WHERE
    current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query == 'SELECT number + number + number FROM numbers(1);'
    AND event_date >= yesterday() AND event_time > now() - interval 10 minute
    LIMIT 1;

SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT sum(number), sum(number + 1), sum(number + 2) FROM numbers(1) GROUP BY number;

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['CompileFunction'] FROM system.query_log WHERE
    current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query == 'SELECT sum(number), sum(number + 1), sum(number + 2) FROM numbers(1) GROUP BY number;'
    AND event_date >= yesterday() AND event_time > now() - interval 10 minute
    LIMIT 1;
