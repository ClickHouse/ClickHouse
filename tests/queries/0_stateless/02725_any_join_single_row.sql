CREATE TABLE join_test
(
    `key` UInt64,
    `value` UInt64
)
ENGINE = Join(ANY, LEFT, key);

CREATE TEMPORARY TABLE initial_table_size AS
    SELECT engine_full, total_rows, total_bytes FROM system.tables WHERE (name = 'join_test') AND (database = currentDatabase());

SELECT engine_full, total_rows, total_bytes < 100_000 FROM initial_table_size;

INSERT INTO join_test (key, value) SELECT 1, number FROM numbers(1);


CREATE TEMPORARY TABLE one_row_table_size AS
    SELECT engine_full, total_rows, total_bytes FROM system.tables WHERE (name = 'join_test') AND (database = currentDatabase());

SELECT engine_full, total_rows, total_bytes < 2 * (SELECT total_bytes FROM initial_table_size) FROM one_row_table_size;

INSERT INTO join_test (key, value) SELECT 1, number FROM numbers(1);
INSERT INTO join_test (key, value) SELECT 1, number FROM numbers(1);
INSERT INTO join_test (key, value) SELECT 1, number FROM numbers(1);
INSERT INTO join_test (key, value) SELECT 1, number FROM numbers(10_000);

SELECT engine_full, total_rows, total_bytes == (SELECT total_bytes FROM one_row_table_size) FROM system.tables WHERE (name = 'join_test') AND (database = currentDatabase());
