select 'testing';

DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv_dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;

CREATE TABLE mv_dst
(
    `key` Int64,
    `value` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window=1000;

CREATE MATERIALIZED VIEW mv
TO mv_dst
AS SELECT
    0 AS key
FROM dst;

SET deduplicate_blocks_in_dependent_materialized_views=1;

set send_logs_level = 'trace';

select 'first attempt';

INSERT INTO dst VALUES (1, 'A');

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;

select 'second attempt';

INSERT INTO dst VALUES (1, 'A');

SELECT
    'from dst',
    *,
    _part
FROM dst
ORDER by all;

SELECT
    'from mv_dst',
    *,
    _part
FROM mv_dst
ORDER by all;

DROP TABLE mv;
DROP TABLE mv_dst;
DROP TABLE dst;
