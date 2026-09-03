-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output and granule counts may differ with random settings.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS test_multi_key_setting;

CREATE TABLE test_multi_key_setting (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts), toDate(ts), ts)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_multi_key_setting SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(24 * 40);

-- With the setting enabled (default), one comparison constrains all three key columns.
SELECT 'comparison, setting on';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_key_setting WHERE ts = toDateTime('2026-01-10 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_multi_key_setting WHERE ts = toDateTime('2026-01-10 00:00:00', 'UTC') SETTINGS force_primary_key = 1;

SET analyze_index_with_multiple_key_columns_per_condition = 0;

-- With the setting disabled, the same comparison constrains at most one key column.
SELECT 'comparison, setting off';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_key_setting WHERE ts = toDateTime('2026-01-10 00:00:00', 'UTC')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_multi_key_setting WHERE ts = toDateTime('2026-01-10 00:00:00', 'UTC') SETTINGS force_primary_key = 1;

-- The same applies to set membership: no wrapped-set atoms are built for the derived key columns.
SELECT 'in, setting off';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_key_setting WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_multi_key_setting WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')) SETTINGS force_primary_key = 1;

SET analyze_index_with_multiple_key_columns_per_condition = 1;

SELECT 'in, setting on';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_key_setting WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC'))) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_multi_key_setting WHERE ts IN (toDateTime('2026-01-10 00:00:00', 'UTC'), toDateTime('2026-02-05 00:00:00', 'UTC')) SETTINGS force_primary_key = 1;

DROP TABLE test_multi_key_setting;

-- A key column computed by a deterministic (non-monotonic) function of the predicate column:
-- with the setting disabled, the direct match on `s` wins and no atom is built for the leading
-- key column `concat(s, '_x')`.
DROP TABLE IF EXISTS test_multi_key_setting_det;

CREATE TABLE test_multi_key_setting_det (s String) ENGINE = MergeTree
ORDER BY (concat(s, '_x'), s)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_multi_key_setting_det SELECT char(97 + number % 26) FROM numbers(100);

SELECT 'deterministic, setting on';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_key_setting_det WHERE s = 'b') WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_multi_key_setting_det WHERE s = 'b' SETTINGS force_primary_key = 1;

SET analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT 'deterministic, setting off';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi_key_setting_det WHERE s = 'b') WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_multi_key_setting_det WHERE s = 'b' SETTINGS force_primary_key = 1;

DROP TABLE test_multi_key_setting_det;
