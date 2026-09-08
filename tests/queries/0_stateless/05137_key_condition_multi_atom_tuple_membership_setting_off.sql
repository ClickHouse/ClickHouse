-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: granule counts may differ with random settings.

-- `analyze_index_with_multiple_key_columns_per_condition = 0` keeps at most one atom per predicate
-- leaf. Set membership of a tuple of key columns then keeps the per-component mapping, which is the
-- analysis that always existed; a key column of type `Tuple` is still used for such a leaf when
-- there is no per-component mapping at all.

SET analyze_index_with_multiple_key_columns_per_condition = 0;
SET optimize_rewrite_has_to_in = 0;

DROP TABLE IF EXISTS test_tuple_membership_setting_off;
DROP TABLE IF EXISTS test_tuple_membership_setting_off_packed;

CREATE TABLE test_tuple_membership_setting_off (s String, x UInt8) ENGINE = MergeTree
ORDER BY tuple(s, x)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tuple_membership_setting_off VALUES ('a', 1), ('b', 5), ('c', 9);

SELECT count() FROM test_tuple_membership_setting_off WHERE (s, x) IN (('b', 5)) SETTINGS force_primary_key = 1;
SELECT count() FROM test_tuple_membership_setting_off WHERE (s, x) NOT IN (('b', 5)) SETTINGS force_primary_key = 1;
SELECT count() FROM test_tuple_membership_setting_off WHERE has([('b', 5)], (s, x)) SETTINGS force_primary_key = 1;

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tuple_membership_setting_off WHERE (s, x) IN (('b', 5)))
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';

-- The tuple of the predicate reaches only a `Tuple`-typed key column, so that mapping is the only
-- one this leaf has. It constrains a single key column and is kept with the setting off.
CREATE TABLE test_tuple_membership_setting_off_packed (s String, x UInt8, y UInt8) ENGINE = MergeTree
ORDER BY (tuple(s, x), y)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_tuple_membership_setting_off_packed VALUES ('a', 1, 1), ('b', 5, 2), ('c', 9, 3);

SELECT count() FROM test_tuple_membership_setting_off_packed WHERE (s, x) IN (('b', 5)) SETTINGS force_primary_key = 1;
SELECT count() FROM test_tuple_membership_setting_off_packed WHERE (s, x) NOT IN (('b', 5)) SETTINGS force_primary_key = 1;
SELECT count() FROM test_tuple_membership_setting_off_packed WHERE has([('b', 5)], (s, x)) SETTINGS force_primary_key = 1;

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN indexes = 1 SELECT count() FROM test_tuple_membership_setting_off_packed WHERE (s, x) IN (('b', 5)))
WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%/%';

DROP TABLE test_tuple_membership_setting_off;
DROP TABLE test_tuple_membership_setting_off_packed;
