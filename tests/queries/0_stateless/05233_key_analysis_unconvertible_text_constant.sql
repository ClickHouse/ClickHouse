-- A text constant that index analysis cannot convert into a DateTime key type must not prune rows.

DROP TABLE IF EXISTS t_unconvertible_monotonic;
DROP TABLE IF EXISTS t_unconvertible_hash;
SET session_timezone = 'UTC';
SET cast_string_to_date_time_mode = 'best_effort';
SET explain_query_plan_default = 'legacy';   -- for the Condition/Granules assertions
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_unconvertible_monotonic;
DROP TABLE IF EXISTS t_unconvertible_hash;

CREATE TABLE t_unconvertible_monotonic (dt DateTime64(6), id Int32) ENGINE = MergeTree ORDER BY toYYYYMMDD(dt) SETTINGS index_granularity = 1;
CREATE TABLE t_unconvertible_hash (dt DateTime64(6), id Int32) ENGINE = MergeTree ORDER BY cityHash64(toHour(dt)) SETTINGS index_granularity = 1;

INSERT INTO t_unconvertible_monotonic VALUES ('2024-01-02 20:00:00', 1), ('2024-01-05 12:00:00', 2);
INSERT INTO t_unconvertible_hash VALUES ('2024-01-02 20:00:00', 1), ('2024-01-05 12:00:00', 2);

-- '2024-01-03 05:00:00+09:00' is the stored 2024-01-02 20:00:00Z, so a parse that drops the offset
-- would compute a key value one day off and skip the granule holding the row.
SELECT '-- monotonic chain key';
SELECT (SELECT count() FROM t_unconvertible_monotonic WHERE dt = '2024-01-03 05:00:00+09:00') AS pruned,
       (SELECT countIf(dt = '2024-01-03 05:00:00+09:00') FROM t_unconvertible_monotonic) AS honest;
SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_unconvertible_monotonic WHERE dt = '2024-01-03 05:00:00+09:00')
WHERE trim(explain) ILIKE 'condition:%' OR trim(explain) ILIKE 'granules:%';

SELECT '-- non-monotonic deterministic key';
SELECT (SELECT count() FROM t_unconvertible_hash WHERE dt = '2024-01-03 05:00:00+09:00') AS pruned,
       (SELECT countIf(dt = '2024-01-03 05:00:00+09:00') FROM t_unconvertible_hash) AS honest;
SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_unconvertible_hash WHERE dt = '2024-01-03 05:00:00+09:00')
WHERE trim(explain) ILIKE 'condition:%' OR trim(explain) ILIKE 'granules:%';

SELECT '-- a constant index analysis can convert still prunes';
SELECT (SELECT count() FROM t_unconvertible_monotonic WHERE dt = '2024-01-02 20:00:00') AS pruned,
       (SELECT countIf(dt = '2024-01-02 20:00:00') FROM t_unconvertible_monotonic) AS honest;
SELECT trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_unconvertible_monotonic WHERE dt = '2024-01-02 20:00:00')
WHERE trim(explain) ILIKE 'condition:%' OR trim(explain) ILIKE 'granules:%';

DROP TABLE t_unconvertible_monotonic;
DROP TABLE t_unconvertible_hash;
