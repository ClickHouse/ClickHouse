DROP TABLE IF EXISTS table_a_b;
DROP TABLE IF EXISTS table_when_b_even;
DROP TABLE IF EXISTS mv_b_even;


SET max_insert_threads=1;
SET update_insert_deduplication_token_in_dependent_materialized_views=1;
SET deduplicate_blocks_in_dependent_materialized_views=1;

SET max_block_size=3;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;


CREATE TABLE table_a_b
    (
        a String,
        b UInt64,
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_a_b;

CREATE TABLE table_when_b_even_wo_dedup
    (
        a String,
        b UInt64,
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=0;
SYSTEM STOP MERGES table_when_b_even;

CREATE MATERIALIZED VIEW mv_b_even_wo_dedup
TO table_when_b_even_wo_dedup
AS
    SELECT a, b
    FROM table_a_b
    WHERE b % 2 = 0;

CREATE TABLE table_when_b_even_dedup
    (
        a String,
        b UInt64,
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_when_b_even;

CREATE MATERIALIZED VIEW mv_b_even_dedup
TO table_when_b_even_dedup
AS
    SELECT a, b
    FROM table_a_b
    WHERE b % 2 = 0;


SELECT 'first insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_a_b
SELECT toString(number DIV  2), number
FROM numbers(5)
SETTINGS send_logs_level='trace';


SELECT 'second insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_a_b
SELECT toString(number DIV  2), number
FROM numbers(5)
SETTINGS send_logs_level='trace';


SELECT 'table_a_b';
SELECT 'count', count() FROM table_a_b;
SELECT _part, count() FROM table_a_b GROUP BY _part;

SELECT 'table_when_b_even_wo_dedup';
SELECT 'count', count() FROM table_when_b_even_wo_dedup;
SELECT _part, count() FROM table_when_b_even_wo_dedup GROUP BY _part;

SELECT 'table_when_b_even_dedup';
SELECT 'count', count() FROM table_when_b_even_dedup;
SELECT _part, count() FROM table_when_b_even_dedup GROUP BY _part;


DROP TABLE mv_b_even;
DROP TABLE table_when_b_even;
DROP TABLE table_a_b;
