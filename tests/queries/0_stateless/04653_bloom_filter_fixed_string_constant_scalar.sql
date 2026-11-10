-- A FixedString(M) constant compares zero-padded, so it matches an unbounded family of stored
-- values ('V0', 'V0\0', 'V0\0\0', ...) while the bloom filter holds one hash per exact value.
-- The index must decline unless the padding maps the constant into exactly one indexed value,
-- i.e. only for a FixedString(N) index with N >= M.

SET allow_suspicious_low_cardinality_types = 1;

-- Direction A: correctness restored. Oracle is an ENGINE = Log table with the same rows.

DROP TABLE IF EXISTS bf_str_log;
DROP TABLE IF EXISTS bf_str_idx;
CREATE TABLE bf_str_log (id UInt64, v String) ENGINE = Log;
CREATE TABLE bf_str_idx (id UInt64, v String, INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_str_log VALUES (0, 'V0'), (1, 'V0\0'), (2, 'V0\0\0'), (3, 'V0X'), (4, 'X');
INSERT INTO bf_str_idx VALUES (0, 'V0'), (1, 'V0\0'), (2, 'V0\0\0'), (3, 'V0X'), (4, 'X');

SELECT 'A1 String idx vs FixedString(2)', (SELECT count() FROM bf_str_log WHERE v = toFixedString('V0', 2)) = (SELECT count() FROM bf_str_idx WHERE v = toFixedString('V0', 2));
SELECT 'A2 String idx vs FixedString(3)', (SELECT count() FROM bf_str_log WHERE v = toFixedString('V0', 3)) = (SELECT count() FROM bf_str_idx WHERE v = toFixedString('V0', 3));
SELECT 'A3 String idx vs FixedString(5)', (SELECT count() FROM bf_str_log WHERE v = toFixedString('V0', 5)) = (SELECT count() FROM bf_str_idx WHERE v = toFixedString('V0', 5));

-- The index genuinely cannot serve the predicate, so it must be reported as unused.
SELECT count() FROM bf_str_idx WHERE v = toFixedString('V0', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A narrower FixedString index keeps the constant's wider padded bytes, which no longer map
-- into the index domain.
DROP TABLE IF EXISTS bf_fs3_log;
DROP TABLE IF EXISTS bf_fs3_idx;
CREATE TABLE bf_fs3_log (id UInt64, v FixedString(3)) ENGINE = Log;
CREATE TABLE bf_fs3_idx (id UInt64, v FixedString(3), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_fs3_log VALUES (0, 'V0'), (1, 'V0X'), (2, 'X');
INSERT INTO bf_fs3_idx VALUES (0, 'V0'), (1, 'V0X'), (2, 'X');

SELECT 'A4 FixedString(3) idx vs FixedString(4)', (SELECT count() FROM bf_fs3_log WHERE v = toFixedString('V0', 4)) = (SELECT count() FROM bf_fs3_idx WHERE v = toFixedString('V0', 4));
SELECT 'A5 FixedString(3) idx vs FixedString(5)', (SELECT count() FROM bf_fs3_log WHERE v = toFixedString('V0', 5)) = (SELECT count() FROM bf_fs3_idx WHERE v = toFixedString('V0', 5));

-- The rule is about byte length, not the constant's declared type: a plain String constant wider
-- than the index is padded by nothing (convertFieldToType never truncates), so it is hashed over
-- more bytes than the index stored while zero-padded comparison still matches at runtime.
-- 'V0\0\0' is a 4-byte String literal against a FixedString(3) index.
SELECT 'A11 FixedString(3) idx vs 4-byte String const', (SELECT count() FROM bf_fs3_log WHERE v = 'V0\0\0') = (SELECT count() FROM bf_fs3_idx WHERE v = 'V0\0\0');
SELECT 'A12 FixedString(3) idx vs 5-byte String const', (SELECT count() FROM bf_fs3_log WHERE v = 'V0\0\0\0') = (SELECT count() FROM bf_fs3_idx WHERE v = 'V0\0\0\0');

SELECT count() FROM bf_fs3_idx WHERE v = 'V0\0\0' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- Wrapped INDEX types: getPrimitiveType strips LowCardinality and Nullable.
DROP TABLE IF EXISTS bf_lc_log;
DROP TABLE IF EXISTS bf_lc_idx;
CREATE TABLE bf_lc_log (id UInt64, v LowCardinality(String)) ENGINE = Log;
CREATE TABLE bf_lc_idx (id UInt64, v LowCardinality(String), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_lc_log VALUES (0, 'V0'), (1, 'V0\0'), (2, 'X');
INSERT INTO bf_lc_idx VALUES (0, 'V0'), (1, 'V0\0'), (2, 'X');
SELECT 'A6 LowCardinality(String) idx vs FixedString(3)', (SELECT count() FROM bf_lc_log WHERE v = toFixedString('V0', 3)) = (SELECT count() FROM bf_lc_idx WHERE v = toFixedString('V0', 3));

DROP TABLE IF EXISTS bf_nl_log;
DROP TABLE IF EXISTS bf_nl_idx;
CREATE TABLE bf_nl_log (id UInt64, v Nullable(String)) ENGINE = Log;
CREATE TABLE bf_nl_idx (id UInt64, v Nullable(String), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_nl_log VALUES (0, 'V0'), (1, 'V0\0'), (2, 'X');
INSERT INTO bf_nl_idx VALUES (0, 'V0'), (1, 'V0\0'), (2, 'X');
SELECT 'A7 Nullable(String) idx vs FixedString(3)', (SELECT count() FROM bf_nl_log WHERE v = toFixedString('V0', 3)) = (SELECT count() FROM bf_nl_idx WHERE v = toFixedString('V0', 3));

DROP TABLE IF EXISTS bf_lcfs_log;
DROP TABLE IF EXISTS bf_lcfs_idx;
CREATE TABLE bf_lcfs_log (id UInt64, v LowCardinality(FixedString(3))) ENGINE = Log;
CREATE TABLE bf_lcfs_idx (id UInt64, v LowCardinality(FixedString(3)), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_lcfs_log VALUES (0, 'V0'), (1, 'X');
INSERT INTO bf_lcfs_idx VALUES (0, 'V0'), (1, 'X');
SELECT 'A8 LowCardinality(FixedString(3)) idx vs FixedString(5)', (SELECT count() FROM bf_lcfs_log WHERE v = toFixedString('V0', 5)) = (SELECT count() FROM bf_lcfs_idx WHERE v = toFixedString('V0', 5));

-- Wrapped CONSTANT types: the guard peels LowCardinality and Nullable off the constant type too,
-- so a LowCardinality(Nullable(FixedString(N))) constant cannot slip past it.
SELECT 'A9 String idx vs LowCardinality(FixedString(3)) const', (SELECT count() FROM bf_str_log WHERE v = CAST(toFixedString('V0', 3) AS LowCardinality(FixedString(3)))) = (SELECT count() FROM bf_str_idx WHERE v = CAST(toFixedString('V0', 3) AS LowCardinality(FixedString(3))));
SELECT 'A10 String idx vs LowCardinality(Nullable(FixedString(3))) const', (SELECT count() FROM bf_str_log WHERE v = CAST(toFixedString('V0', 3) AS LowCardinality(Nullable(FixedString(3))))) = (SELECT count() FROM bf_str_idx WHERE v = CAST(toFixedString('V0', 3) AS LowCardinality(Nullable(FixedString(3)))));

-- Variant and Dynamic erase the active type: the declared type stays Variant/Dynamic while the
-- constant Field already holds the nested padded bytes, so a type-based FixedString test alone
-- cannot see them and the padding-unsound cell must be declined on the wrapper instead.
SELECT 'A13 String idx vs Variant(FixedString(3)) const', (SELECT count() FROM bf_str_log WHERE v = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64))) = (SELECT count() FROM bf_str_idx WHERE v = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)));
SELECT 'A14 String idx vs Dynamic(FixedString(3)) const', (SELECT count() FROM bf_str_log WHERE v = CAST(toFixedString('V0', 3) AS Dynamic)) = (SELECT count() FROM bf_str_idx WHERE v = CAST(toFixedString('V0', 3) AS Dynamic));

SELECT count() FROM bf_str_idx WHERE v = CAST(toFixedString('V0', 3) AS Variant(FixedString(3), UInt64)) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- Direction B: pruning is preserved wherever the conversion is faithful. Each row asserts the
-- granule REDUCTION extracted from the plan text rather than pinning an exact plan rendering in
-- the .reference, so it keeps testing "the index still prunes" across plan-format churn.

DROP TABLE IF EXISTS bf_keys_str;
DROP TABLE IF EXISTS bf_keys_fs3;
CREATE TABLE bf_keys_str (id UInt64, v String, INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE bf_keys_fs3 (id UInt64, v FixedString(3), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bf_keys_str SELECT number, concat('k', toString(number)) FROM numbers(64);
INSERT INTO bf_keys_fs3 SELECT number, concat('k', toString(number)) FROM numbers(64);

SELECT 'B1 String idx vs String const still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_keys_str WHERE v = 'k7')
WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

SELECT 'B2 FixedString(3) idx vs String const still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_keys_fs3 WHERE v = 'k7')
WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

SELECT 'B3 FixedString(3) idx vs FixedString(2) const still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_keys_fs3 WHERE v = toFixedString('k7', 2))
WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

SELECT 'B4 FixedString(3) idx vs FixedString(3) const still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_keys_fs3 WHERE v = toFixedString('k7', 3))
WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

-- A plain String constant no wider than the FixedString index is padded into exactly one indexed
-- value, so the widened width rule must not start declining these sound cells.
SELECT 'B6 FixedString(3) idx vs 3-byte String const still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_keys_fs3 WHERE v = 'k7\0')
WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

-- The declined cells must stop pruning: these rows are 1 before the fix and 0 after it, which is
-- what makes the Direction B assertions above non-vacuous.
SELECT 'B5 String idx vs FixedString(3) const does not prune', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_keys_str WHERE v = toFixedString('k7', 3))
WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

SELECT 'B7 FixedString(3) idx vs 4-byte String const does not prune', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM bf_keys_fs3 WHERE v = 'k7\0\0')
WHERE explain LIKE '%Granules: %/%' AND toUInt64OrZero(extract(explain, 'Granules: (\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \d+/(\d+)'));

DROP TABLE bf_str_log;
DROP TABLE bf_str_idx;
DROP TABLE bf_fs3_log;
DROP TABLE bf_fs3_idx;
DROP TABLE bf_lc_log;
DROP TABLE bf_lc_idx;
DROP TABLE bf_nl_log;
DROP TABLE bf_nl_idx;
DROP TABLE bf_lcfs_log;
DROP TABLE bf_lcfs_idx;
DROP TABLE bf_keys_str;
DROP TABLE bf_keys_fs3;
