-- A plain `ANY LEFT JOIN` against an `ENGINE = Join` table with an always-false `WHERE`: the filter is
-- `FALSE` for the rows the join does not match, so `query_plan_convert_any_join_to_semi_or_anti_join`
-- turned the join into a `SEMI LEFT` one, which neither `full_sorting_merge` nor `partial_merge` can
-- execute, and the query failed with `NOT_IMPLEMENTED` while the same query with
-- `query_plan_enable_optimizations = 0` ran. A plan rewrite must not decide whether a query runs.

DROP TABLE IF EXISTS t_storage_join_left;
DROP TABLE IF EXISTS t_storage_join_right;

CREATE TABLE t_storage_join_left (k UInt32, g UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_storage_join_left SELECT number, number FROM numbers(100);

CREATE TABLE t_storage_join_right (k UInt32, jv Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_storage_join_right SELECT number, number FROM numbers(50);

SELECT 'always-false WHERE under each algorithm';
SELECT count() FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE 0
    SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE 0
    SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE 0
    SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE 0
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_enable_optimizations = 0;

SELECT 'without the WHERE';
SELECT count() FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k)
    SETTINGS join_algorithm = 'full_sorting_merge';

-- A filter that is `FALSE` only for the not matched rows is the case the conversion is meant for; the
-- result must be the same whichever algorithm is enabled.
SELECT 'a filter that only drops the not matched rows';
SELECT count(), sum(r.jv) FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE r.jv > 0
    SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count(), sum(r.jv) FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE r.jv > 0
    SETTINGS join_algorithm = 'partial_merge';
SELECT count(), sum(r.jv) FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE r.jv > 0
    SETTINGS join_algorithm = 'hash';
SELECT count(), sum(r.jv) FROM t_storage_join_left AS l ANY LEFT JOIN t_storage_join_right AS r USING (k) WHERE r.jv > 0
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_enable_optimizations = 0;

DROP TABLE t_storage_join_left;
DROP TABLE t_storage_join_right;
