DROP TABLE IF EXISTS test;

CREATE TABLE table_for_join_with
    (
        a_join String,
        b UInt64
    )
    ENGINE = MergeTree()
    ORDER BY (a_join, b);

INSERT INTO table_for_join_with
    SELECT 'joined_' || toString(number), number
    FROM numbers(10);
SELECT 'table_for_join_with';
SELECT a_join, b, _part FROM table_for_join_with ORDER BY _part, a_join, b;


CREATE TABLE table_a_b
    (
        a_src String,
        b UInt64
    )
    ENGINE = MergeTree()
    ORDER BY (a_src, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_a_b;

CREATE TABLE table_when_b_even_dedup
    (
        a_src String CODEC(NONE),
        a_join String CODEC(NONE),
        b UInt64 CODEC(NONE)
    )
    ENGINE = MergeTree()
    ORDER BY (a_src, a_join, b)
    SETTINGS non_replicated_deduplication_window=10000;
SYSTEM STOP MERGES table_when_b_even_dedup;

CREATE MATERIALIZED VIEW mv_b_even_dedup
    TO table_when_b_even_dedup
    AS
        SELECT a_src, a_join, b
            FROM table_a_b
            FULL OUTER JOIN table_for_join_with
            ON table_a_b.b = table_for_join_with.b AND table_a_b.b % 2 = 0
            ORDER BY a_src, a_join, b;

CREATE TABLE table_when_b_even_wo_dedup
    (
        a_src String CODEC(NONE),
        a_join String CODEC(NONE),
        b UInt64 CODEC(NONE)
    )
    ENGINE = MergeTree()
    ORDER BY (a_src, a_join, b)
    SETTINGS non_replicated_deduplication_window=0;
SYSTEM STOP MERGES table_when_b_even_wo_dedup;

CREATE MATERIALIZED VIEW mv_b_even_wo_dedup
    TO table_when_b_even_wo_dedup
    AS
        SELECT a_src, a_join, b
            FROM table_a_b
            FULL OUTER JOIN table_for_join_with
            ON table_a_b.b = table_for_join_with.b AND table_a_b.b % 2 = 0
            ORDER BY a_src, a_join, b;


SET max_insert_threads=1;
SET update_insert_deduplication_token_in_dependent_materialized_views=1;
SET deduplicate_blocks_in_dependent_materialized_views=1;

SET max_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;


SELECT 'first insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_a_b
SELECT 'source_' || toString(number), number
FROM numbers(5)
SETTINGS send_logs_level='trace';


SELECT 'second insert'
SETTINGS send_logs_level='trace';

INSERT INTO table_a_b
SELECT 'source_' || toString(number), number
FROM numbers(5)
SETTINGS send_logs_level='trace';


SELECT 'table_a_b';
SELECT 'count', count() FROM table_a_b;
SELECT _part, count() FROM table_a_b GROUP BY _part;

SELECT 'table_when_b_even_dedup, here the result if join is deduplicated inside one request, it is not correct';
SELECT 'count', count() FROM table_when_b_even_dedup;
SELECT _part, count() FROM table_when_b_even_dedup GROUP BY _part;

SELECT 'table_when_b_even_wo_dedup';
SELECT 'count', count() FROM table_when_b_even_wo_dedup;
SELECT _part, count() FROM table_when_b_even_wo_dedup GROUP BY _part ORDER BY _part;


DROP TABLE mv_b_even_dedup;
DROP TABLE table_when_b_even_dedup;
DROP TABLE mv_b_even_wo_dedup;
DROP TABLE table_when_b_even_wo_dedup;
DROP TABLE table_a_b;
