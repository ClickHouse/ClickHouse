DROP TABLE IF EXISTS test;

CREATE TABLE table_a_b
    (
        a String,
        b UInt64
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_a_b;

CREATE TABLE table_when_b_even
    (
        a String CODEC(NONE),
        b UInt64 CODEC(NONE)
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_when_b_even;

CREATE MATERIALIZED VIEW mv_b_even
    TO table_when_b_even
    AS
    SELECT a, b
        FROM table_a_b
        WHERE b % 2 = 0;


SET max_insert_threads=1;
SET update_insert_deduplication_token_in_dependent_materialized_views=1;
SET deduplicate_blocks_in_dependent_materialized_views=1;

SET max_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;


SELECT 'first insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_a_b
SELECT 'source_' || toString(1), 1
FROM numbers(5)
SETTINGS send_logs_level='trace';

SELECT 'table_a_b, it deduplicates rows within one insert, it is wrong';
SELECT 'count', count() FROM table_a_b;
SELECT _part, count() FROM table_a_b GROUP BY _part ORDER BY _part;

SELECT 'table_when_b_even';
SELECT 'count', count() FROM table_when_b_even;
SELECT _part, count() FROM table_when_b_even GROUP BY _part ORDER BY _part;


SELECT 'second insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_a_b
SELECT 'source_' || toString(1), 1
FROM numbers(5)
SETTINGS send_logs_level='trace';

SELECT 'table_a_b';
SELECT 'count', count() FROM table_a_b;
SELECT _part, count() FROM table_a_b GROUP BY _part;

SELECT 'table_when_b_even';
SELECT 'count', count() FROM table_when_b_even;
SELECT _part, count() FROM table_when_b_even GROUP BY _part;


DROP TABLE mv_b_even;
DROP TABLE table_when_b_even;
DROP TABLE table_a_b;
