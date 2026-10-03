-- A text index on `mapKeys(m)` / `mapValues(m)` is used for `m[key] = value` also when the constant key
-- is wrapped in `Nullable`, `LowCardinality` or `LowCardinality(Nullable)`. A non-NULL constant key
-- reads the value-type default ('') for an absent key, not NULL, so the index can still prune granules.
-- The `arrayElement(m, key)` form is exercised with `materialize` on the key, which defeats the rewrite
-- to a map subcolumn; without it the analyzer produces `_CAST(m.key_<key>, 'Nullable(String)')`.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
-- Keep a `ReadFromMergeTree` step in the plan so the granule count is reported.
SET query_plan_optimize_count_from_text_index = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX idx_vals mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8;

INSERT INTO tab SELECT map('key' || toString(number % 1000), 'value' || toString(number % 1000)) FROM numbers(1024);

SELECT '-- results are correct and independent of the key type wrapper';
SELECT count() FROM tab WHERE m['key5'] = 'value5';
SELECT count() FROM tab WHERE m[CAST('key5' AS Nullable(String))] = 'value5';
SELECT count() FROM tab WHERE m[CAST('key5' AS LowCardinality(String))] = 'value5';
SELECT count() FROM tab WHERE m[CAST('key5' AS LowCardinality(Nullable(String)))] = 'value5';
SELECT count() FROM tab WHERE m[CAST('missing' AS Nullable(String))] = 'value5';
SELECT count() FROM tab WHERE m[CAST(NULL AS Nullable(String))] = 'value5';
SELECT count() FROM tab WHERE m[CAST(NULL AS LowCardinality(Nullable(String)))] = 'value5';

SELECT '-- mapValues index prunes granules for a wrapped constant key, arrayElement form';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS Nullable(String)))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(String)))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(Nullable(String))))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';

SELECT '-- mapValues index prunes granules for a wrapped constant key, subcolumn form';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[CAST('key5' AS Nullable(String))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[CAST('key5' AS LowCardinality(String))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[CAST('key5' AS LowCardinality(Nullable(String)))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';

SELECT '-- mapKeys index prunes granules for a wrapped constant key, arrayElement form';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS Nullable(String)))] != '') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(String)))] != '') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(Nullable(String))))] != '') WHERE explain ILIKE '%Granules: 2/128%';

SELECT '-- mapKeys index prunes granules for a wrapped constant key, subcolumn form';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[CAST('key5' AS Nullable(String))] != '') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[CAST('key5' AS LowCardinality(String))] != '') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[CAST('key5' AS LowCardinality(Nullable(String)))] != '') WHERE explain ILIKE '%Granules: 2/128%';

SELECT '-- a NULL constant key does not use the mapKeys index: arrayElement returns NULL, not the default';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST(NULL AS Nullable(String)))] != '') WHERE explain ILIKE '%idx_keys%';

-- A cast of the map value that changes its bytes must not reuse the mapValues index: the index stores
-- the raw String token, while `CAST(m[key], 'FixedString(N)')` pads the value with zero bytes. Only the
-- casts that keep the bytes (adding `Nullable` or `LowCardinality`) may reuse it.
SELECT '-- FixedString cast on the value does not use the mapValues index, Nullable cast does';
SELECT count() FROM tab WHERE CAST(m['key5'], 'FixedString(6)') = toFixedString('value5', 6);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE CAST(m[materialize('key5')], 'FixedString(6)') = toFixedString('value5', 6)) WHERE explain ILIKE '%idx_vals%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE CAST(m[materialize('key5')], 'Nullable(String)') = 'value5') WHERE explain ILIKE '%idx_vals%';

DROP TABLE tab;
