-- A row store is only built when reading one output row's payload from it saves at least two cache
-- misses over reading it column by column. Column by column each plane is a miss of its own, and a
-- `Nullable` column is two planes; from the store the misses are the lines one row spans.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET enable_hash_join_row_store = 1;
SET min_rows_ratio_for_hash_join_row_store = 0; -- take the output ratio out of the decision
SET collect_hash_table_stats_during_joins = 0;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0; -- so the join keys are not saved beside the payload

CREATE TABLE probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE asof_probe (k UInt64, ts UInt64) ENGINE = MergeTree ORDER BY tuple();

-- 2 misses column by column against 1 for a 16 byte row: saves 1.
CREATE TABLE build_2_u64 (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
-- 3 misses column by column against 1 for a 24 byte row: saves 2.
CREATE TABLE build_3_u64 (k UInt64, a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY tuple();
-- 4 misses column by column, a `Nullable` column having a null map plane of its own, against 1 for an 18 byte row: saves 3.
CREATE TABLE build_2_nullable (k UInt64, a Nullable(UInt64), b Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
-- 2 misses column by column against 1 for a 64 byte row: saves 1.
CREATE TABLE build_2_fs32 (k UInt64, a FixedString(32), b FixedString(32)) ENGINE = MergeTree ORDER BY tuple();
-- 4 misses column by column against 2 for a 128 byte row: saves 2.
CREATE TABLE build_4_fs32 (k UInt64, a FixedString(32), b FixedString(32), c FixedString(32), d FixedString(32)) ENGINE = MergeTree ORDER BY tuple();
-- An ASOF join emits its inequality key: 3 misses against 1.
CREATE TABLE build_asof (k UInt64, ts UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO probe SELECT number % 100 FROM numbers(1000);
INSERT INTO asof_probe SELECT number % 50, number FROM numbers(500);
INSERT INTO build_2_u64 SELECT number, number, number FROM numbers(100);
INSERT INTO build_3_u64 SELECT number, number, number, number FROM numbers(100);
INSERT INTO build_2_nullable SELECT number, if(number % 10 = 0, NULL, number), number FROM numbers(100);
INSERT INTO build_2_fs32 SELECT number, toFixedString(toString(number), 32), toFixedString(toString(number), 32) FROM numbers(100);
INSERT INTO build_4_fs32 SELECT number, toFixedString(toString(number), 32), toFixedString(toString(number), 32), toFixedString(toString(number), 32), toFixedString(toString(number), 32) FROM numbers(100);
INSERT INTO build_asof SELECT number % 50, number, number, number FROM numbers(200);

SELECT a, b FROM probe p JOIN build_2_u64 r ON p.k = r.k FORMAT Null SETTINGS log_comment = 'build_2_u64';
SELECT a, b, c FROM probe p JOIN build_3_u64 r ON p.k = r.k FORMAT Null SETTINGS log_comment = 'build_3_u64';
SELECT a, b FROM probe p JOIN build_2_nullable r ON p.k = r.k FORMAT Null SETTINGS log_comment = 'build_2_nullable';
SELECT a, b FROM probe p JOIN build_2_fs32 r ON p.k = r.k FORMAT Null SETTINGS log_comment = 'build_2_fs32';
SELECT a, b, c, d FROM probe p JOIN build_4_fs32 r ON p.k = r.k FORMAT Null SETTINGS log_comment = 'build_4_fs32';

-- A column that is saved but never read back out of the store is left out of it, like the key of a `RIGHT` join.
SELECT a, b FROM probe p RIGHT JOIN build_2_u64 r ON p.k = r.k FORMAT Null SETTINGS log_comment = 'saved_key_2_u64';
SELECT a, b, c FROM probe p RIGHT JOIN build_3_u64 r ON p.k = r.k FORMAT Null SETTINGS log_comment = 'saved_key_3_u64';
SELECT r.ts, r.a, r.b FROM asof_probe p ASOF JOIN build_asof r ON p.k = r.k AND p.ts >= r.ts FORMAT Null SETTINGS log_comment = 'asof_key_in_store';

SYSTEM FLUSH LOGS text_log, query_log;

SELECT comment, anyIf(extract(message, 'with (\\d+) columns'), message LIKE 'Initialized Row store%') AS row_store_columns
FROM
(
    SELECT log_comment AS comment, query_id
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
      AND log_comment IN ('build_2_u64', 'build_3_u64', 'build_2_nullable', 'build_2_fs32', 'build_4_fs32',
                          'saved_key_2_u64', 'saved_key_3_u64', 'asof_key_in_store')
) AS q
LEFT JOIN system.text_log AS t ON t.query_id = q.query_id
GROUP BY comment
ORDER BY comment;

DROP TABLE probe;
DROP TABLE asof_probe;
DROP TABLE build_2_u64;
DROP TABLE build_3_u64;
DROP TABLE build_2_nullable;
DROP TABLE build_2_fs32;
DROP TABLE build_4_fs32;
DROP TABLE build_asof;
