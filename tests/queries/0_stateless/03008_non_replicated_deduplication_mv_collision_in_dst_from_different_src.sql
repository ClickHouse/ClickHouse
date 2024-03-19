DROP TABLE IF EXISTS test;

CREATE TABLE table_source
    (
        a String,
        b UInt64
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_source;

CREATE TABLE table_dst_dedup
    (
        a String,
        b UInt64
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_dst_dedup;

CREATE MATERIALIZED VIEW mv_b_even_dedup
    TO table_dst_dedup
    AS
    SELECT a, b
        FROM table_source
        WHERE b % 2 = 0;

CREATE MATERIALIZED VIEW mv_b_even_even_dedup
    TO table_dst_dedup
    AS
    SELECT a, b
        FROM table_source
        WHERE b % 4 = 0;

CREATE TABLE table_dst_wo_dedup
    (
        a String,
        b UInt64
    )
    ENGINE = MergeTree()
    ORDER BY (a, b)
    SETTINGS non_replicated_deduplication_window=0;
SYSTEM STOP MERGES table_dst_wo_dedup;

CREATE MATERIALIZED VIEW mv_b_even_wo_dedup
    TO table_dst_wo_dedup
    AS
    SELECT a, b
        FROM table_source
        WHERE b % 2 = 0;

CREATE MATERIALIZED VIEW mv_b_even_wo_even_dedup
    TO table_dst_wo_dedup
    AS
    SELECT a, b
        FROM table_source
        WHERE b % 4 = 0;


SET max_insert_threads=1;
SET update_insert_deduplication_token_in_dependent_materialized_views=1;
SET deduplicate_blocks_in_dependent_materialized_views=1;

SET max_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;


SELECT 'first insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_source
SELECT 'source_' || toString(number), number
FROM numbers(8)
SETTINGS send_logs_level='trace';

SELECT 'table_source';
SELECT 'count', count() FROM table_source;
SELECT _part, count() FROM table_source GROUP BY _part ORDER BY _part;

SELECT 'table_dst_dedup';
SELECT 'count', count() FROM table_dst_dedup;
SELECT _part, count() FROM table_dst_dedup GROUP BY _part ORDER BY _part;

SELECT 'table_dst_wo_dedup';
SELECT 'count', count() FROM table_dst_wo_dedup;
SELECT _part, count() FROM table_dst_wo_dedup GROUP BY _part ORDER BY _part;


SELECT 'second insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_source
SELECT 'source_' || toString(number), number
FROM numbers(8)
SETTINGS send_logs_level='trace';

SELECT 'table_source';
SELECT 'count', count() FROM table_source;
SELECT _part, count() FROM table_source GROUP BY _part ORDER BY _part;

SELECT 'table_dst_dedup, block from different mv is deduplicated, it is wrong';
SELECT 'count', count() FROM table_dst_dedup;
SELECT _part, count() FROM table_dst_dedup GROUP BY _part ORDER BY _part;

SELECT 'table_dst_wo_dedup';
SELECT 'count', count() FROM table_dst_wo_dedup;
SELECT _part, count() FROM table_dst_wo_dedup GROUP BY _part ORDER BY _part;


DROP TABLE mv_b_even_dedup;
DROP TABLE mv_b_even_even_dedup;
DROP TABLE mv_b_even_wo_dedup;
DROP TABLE mv_b_even_even_wo_dedup;
DROP TABLE table_dst_dedup;
DROP TABLE table_dst_wo_dedup;
DROP TABLE table_source;
