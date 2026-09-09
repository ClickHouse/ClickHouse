-- https://github.com/ClickHouse/ClickHouse/issues/118738
-- With join_use_nulls, a selected right join key is joined on as its Nullable output column, so the right
-- side carries one column that the join restores from the left key, instead of the plain key plus the
-- Nullable wrapper as a payload column.

SET join_use_nulls = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
CREATE TABLE l (k String, v UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r (k String, w UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO l VALUES ('a', 1), ('b', 2), ('c', 3), ('c', 4);
INSERT INTO r VALUES ('a', 10), ('c', 30), ('c', 31), ('d', 40);

SELECT '-- right side of the plan carries only the Nullable key';
SELECT trimLeft(explain) FROM (
    EXPLAIN header = 1
    SELECT r.k, sum(l.v) FROM l LEFT JOIN r ON l.k = r.k GROUP BY r.k
    SETTINGS explain_query_plan_default = 'legacy', query_plan_join_swap_table = 0, join_algorithm = 'hash'
) WHERE explain LIKE '%Right Pre Join Actions%' OR explain LIKE '%__table2.k%';

SELECT '-- LEFT';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT toTypeName(r.k), count() FROM l LEFT JOIN r ON l.k = r.k GROUP BY 1 SETTINGS join_algorithm = 'hash';
SELECT '-- LEFT, key only';
SELECT r.k FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- LEFT ANY';
SELECT l.k, r.k FROM l LEFT ANY JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- LEFT SEMI';
SELECT l.k, r.k FROM l LEFT SEMI JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- LEFT ANTI';
SELECT l.k, r.k FROM l LEFT ANTI JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- FULL';
SELECT l.k, r.k, r.w FROM l FULL JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- LEFT with residual ON condition';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k AND r.w > 30 ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- LEFT with a residual ON condition on the right key itself';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k AND r.k != 'a' ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- LEFT with WHERE on the right key';
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k IS NULL ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.k = 'c' ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- null-safe key';
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k IS NOT DISTINCT FROM r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- parallel_hash';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';
SELECT l.k, r.k, r.w FROM l FULL JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'parallel_hash';
SELECT '-- grace_hash';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4;
SELECT l.k, r.k, r.w FROM l FULL JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 4;
SELECT '-- spilling wrapper';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 100000000;
SELECT '-- partial_merge';
SELECT trimLeft(explain) FROM (
    EXPLAIN header = 1
    SELECT r.k, sum(l.v) FROM l LEFT JOIN r ON l.k = r.k GROUP BY r.k
    SETTINGS explain_query_plan_default = 'legacy', query_plan_join_swap_table = 0, join_algorithm = 'partial_merge'
) WHERE explain LIKE '%Right Pre Join Actions%' OR explain LIKE '%__table2.k%';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT l.k, r.k FROM l LEFT ANY JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT l.k, r.k, r.w FROM l FULL JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT l.k, r.k FROM l LEFT JOIN (SELECT toLowCardinality(k) AS k FROM r) AS r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'partial_merge';
SELECT toTypeName(r.k) FROM l LEFT JOIN (SELECT toLowCardinality(k) AS k FROM r) AS r ON l.k = r.k LIMIT 1 SETTINGS join_algorithm = 'partial_merge';
SELECT '-- auto, switched to partial_merge';
SELECT trimLeft(explain) FROM (
    EXPLAIN header = 1
    SELECT r.k, sum(l.v) FROM l LEFT JOIN r ON l.k = r.k GROUP BY r.k
    SETTINGS explain_query_plan_default = 'legacy', query_plan_join_swap_table = 0, join_algorithm = 'auto'
) WHERE explain LIKE '%Right Pre Join Actions%' OR explain LIKE '%__table2.k%';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'auto', max_rows_in_join = 2, join_overflow_mode = 'break';
SELECT l.k, r.k, r.w FROM l FULL JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'auto', max_rows_in_join = 2, join_overflow_mode = 'break';
SELECT l.k, r.k FROM l LEFT JOIN (SELECT toLowCardinality(k) AS k FROM r) AS r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'auto', max_rows_in_join = 2, join_overflow_mode = 'break';
SELECT '-- full_sorting_merge';
SELECT trimLeft(explain) FROM (
    EXPLAIN header = 1
    SELECT r.k, sum(l.v) FROM l LEFT JOIN r ON l.k = r.k GROUP BY r.k
    SETTINGS explain_query_plan_default = 'legacy', query_plan_join_swap_table = 0, join_algorithm = 'full_sorting_merge'
) WHERE explain LIKE '%Right Pre Join Actions%' OR explain LIKE '%__table2.k%';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';
SELECT '-- LowCardinality key';
SELECT l.k, r.k FROM l LEFT JOIN (SELECT toLowCardinality(k) AS k FROM r) AS r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT toTypeName(r.k) FROM l LEFT JOIN (SELECT toLowCardinality(k) AS k FROM r) AS r ON l.k = r.k LIMIT 1 SETTINGS join_algorithm = 'hash';
SELECT '-- key with a type conversion';
SELECT l.v, r.w FROM l LEFT JOIN r ON l.v * 10 = r.w ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT l.v, r.w FROM l LEFT JOIN r ON toInt64(l.v) * 10 = r.w ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- derived right key';
SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = lower(upper(r.k)) ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- WITH TOTALS';
SELECT r.k, sum(l.v) FROM l LEFT JOIN r ON l.k = r.k GROUP BY r.k WITH TOTALS ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT k, sum(v), sum(w) FROM (SELECT k, sum(v) AS v FROM l GROUP BY k WITH TOTALS) AS l FULL JOIN (SELECT k, sum(w) AS w FROM r GROUP BY k WITH TOTALS) AS r USING (k) GROUP BY k WITH TOTALS ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT l.k, r.k FROM (SELECT k FROM l GROUP BY k WITH TOTALS) AS l LEFT JOIN (SELECT k FROM r GROUP BY k WITH TOTALS) AS r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash';
SELECT '-- swapped';
SELECT l.k, r.k, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY ALL SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 1;

DROP TABLE l;
DROP TABLE r;
