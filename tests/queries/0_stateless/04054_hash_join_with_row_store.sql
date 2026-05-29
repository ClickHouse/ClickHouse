-- Tests for storing the hash table payload of a hash join in row-major form.

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
DROP TABLE IF EXISTS right_asof;
DROP TABLE IF EXISTS right_storage_join;

CREATE TABLE left (k Int64, t DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right (k Int64, v2 Nullable(Int64), s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE right_asof (k Int64, t DateTime('UTC'), v2 Nullable(Int64), s String) ENGINE = MergeTree ORDER BY (k, t);
CREATE TABLE right_storage_join (k Int64, v2 Nullable(Int64), s String) ENGINE = Join(ANY, LEFT, k);

INSERT INTO left SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number FROM numbers(10);
INSERT INTO right SELECT number + 7, number, toString(number) FROM numbers(5);
INSERT INTO right_asof SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number, number, toString(number) FROM numbers(5);
INSERT INTO right_storage_join SELECT * FROM right;

SET join_algorithm = 'hash';
SET min_columns_for_hash_join_row_store = 1;

-- Disable other post build phase optimizations.
SET allow_experimental_join_right_table_sorting = 0;
SET enable_join_fixed_hash_table_conversion = 0;

SELECT '--- Verify row store enabled / disabled ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null SETTINGS log_comment = 'rs_04054_enabled';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null SETTINGS max_bytes_for_hash_join_row_store = 1, log_comment = 'rs_04054_max_bytes';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k FORMAT Null SETTINGS min_columns_for_hash_join_row_store = 0, log_comment = 'rs_04054_min_cols_zero';

SYSTEM FLUSH LOGS;

SELECT
    sumIf(num, log_comment = 'rs_04054_enabled') > 0 AS enabled,
    sumIf(num, log_comment = 'rs_04054_max_bytes') AS disabled_by_max_bytes,
    sumIf(num, log_comment = 'rs_04054_min_cols_zero') AS disabled_by_min_columns
FROM (
    SELECT q.log_comment AS log_comment, count() AS num
    FROM system.query_log q
    INNER JOIN system.processors_profile_log p USING (query_id)
    WHERE q.log_comment IN ('rs_04054_enabled', 'rs_04054_max_bytes', 'rs_04054_min_cols_zero')
      AND q.type = 'QueryFinish'
      AND p.name = 'FinalizingRightJoinSide'
    GROUP BY q.log_comment
);

SELECT '--- Verify different join types ---';
SELECT '--- INNER JOIN ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- LEFT JOIN ---';
SELECT * FROM left l LEFT JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- RIGHT JOIN ---';
SELECT * FROM left l RIGHT JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- FULL JOIN ---';
SELECT * FROM left l FULL JOIN right r ON l.k = r.k ORDER BY ALL;

SELECT '--- ASOF JOIN ---';
SELECT * FROM left l ASOF JOIN right_asof r ON l.k = r.k AND l.t >= r.t ORDER BY ALL;

SELECT '--- Parallel hash JOIN ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';

SELECT '--- Row-list JOIN output ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_output_by_rowlist_perkey_rows_threshold = 0;

SELECT '--- Join with block splitting ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS max_joined_block_size_rows = 2, joined_block_split_single_row = 1;

SELECT '--- joinGet / joinGetOrNull on Join engine storage ---';
SELECT k,
       joinGet({CLICKHOUSE_DATABASE:String} || '.right_storage_join', 'v2', k),
       joinGetOrNull({CLICKHOUSE_DATABASE:String} || '.right_storage_join', 'v2', k)
FROM left ORDER BY ALL;

SELECT '--- Join with spilling ---';
SELECT * FROM left l INNER JOIN right r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash,grace_hash', max_bytes_before_external_join = 1;

DROP TABLE right_storage_join;
DROP TABLE right_asof;
DROP TABLE right;
DROP TABLE left;
