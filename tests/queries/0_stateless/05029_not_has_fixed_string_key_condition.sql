-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ with random settings.

SET explain_query_plan_default = 'legacy';

-- { echo }

DROP TABLE IF EXISTS test_not_has_fs;
CREATE TABLE test_not_has_fs (fs FixedString(3)) ENGINE = MergeTree
ORDER BY fs
SETTINGS index_granularity = 1;

INSERT INTO test_not_has_fs VALUES (toFixedString('V0', 3)), (toFixedString('abc', 3));

-- `has` compares the original `String` element with the `FixedString` value byte-for-byte, so the
-- unpadded literal 'V0' does not match the stored 'V0\0'. The set index would pad the element to
-- `FixedString(3)` and match, so no set atom must be built: `notHas` must return the row.
SELECT count() FROM test_not_has_fs WHERE notHas(['V0'], fs) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_fs WHERE has(['V0'], fs) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_fs WHERE notHas(['V0'], fs) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

-- With an element of the exact key type the set atom is allowed and prunes.
SELECT count() FROM test_not_has_fs WHERE notHas([toFixedString('V0', 3)], fs) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_fs WHERE has([toFixedString('V0', 3)], fs) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_fs WHERE notHas([toFixedString('V0', 3)], fs) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_not_has_fs;

-- The other direction: `FixedString` element against a `String` key is declined as well, because
-- the cast to `String` keeps the zero bytes while comparisons could involve padding differences.
DROP TABLE IF EXISTS test_not_has_s;
CREATE TABLE test_not_has_s (s String) ENGINE = MergeTree
ORDER BY s
SETTINGS index_granularity = 1;

INSERT INTO test_not_has_s VALUES ('V0'), ('abc');

SELECT count() FROM test_not_has_s WHERE notHas([toFixedString('V0', 3)], s) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_s WHERE has([toFixedString('V0', 3)], s) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_s WHERE notHas([toFixedString('V0', 3)], s) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

-- `String` element against a `String` key keeps the exact set atom.
SELECT count() FROM test_not_has_s WHERE notHas(['V0'], s) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() FROM test_not_has_s WHERE has(['V0'], s) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_not_has_s WHERE notHas(['V0'], s) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_not_has_s;
