-- Tests that a text index on mapKeys(m) / mapValues(m) is used for `m[key] = value`
-- when the constant map key is wrapped in Nullable, LowCardinality, or
-- LowCardinality(Nullable). Such a wrapper is safe: a non-NULL constant key returns the
-- value-type default ('') for an absent key (not NULL), so the index can still prune
-- granules. materialize() is used on the key in the EXPLAIN checks to defeat constant
-- folding into a map subcolumn reference, so the arrayElement(map, wrapped_key) path is
-- actually exercised.

SET enable_analyzer = 1;

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

SELECT '-- mapValues index prunes granules for wrapped constant key (issue #110031)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS Nullable(String)))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(String)))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(Nullable(String))))] = 'value5') WHERE explain ILIKE '%Granules: 2/128%';

SELECT '-- mapKeys index prunes granules for wrapped constant key (issue #110031)';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS Nullable(String)))] != '') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(String)))] != '') WHERE explain ILIKE '%Granules: 2/128%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE m[materialize(CAST('key5' AS LowCardinality(Nullable(String))))] != '') WHERE explain ILIKE '%Granules: 2/128%';

-- A byte-changing cast on the map VALUE must NOT reuse the mapValues text index: the index
-- stores the raw String token, but e.g. CAST(m[key], 'FixedString(N)') pads with trailing zero
-- bytes, so searching the index for the padded FixedString token could wrongly prune a matching
-- granule. Only nullability/LowCardinality wrappers (which preserve the indexed bytes) may reuse
-- the index. The results below prove the FixedString cast still returns the matching row, and the
-- EXPLAIN shows idx_vals is NOT used for it (while a byte-preserving Nullable cast still uses it).
SELECT '-- byte-changing FixedString cast on value must NOT wrongly prune (issue #110031 review)';
SELECT count() FROM tab WHERE CAST(m['key5'], 'FixedString(6)') = toFixedString('value5', 6);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE CAST(m[materialize('key5')], 'FixedString(6)') = toFixedString('value5', 6)) WHERE explain ILIKE '%idx_vals%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE CAST(m[materialize('key5')], 'Nullable(String)') = 'value5') WHERE explain ILIKE '%idx_vals%';

DROP TABLE tab;
