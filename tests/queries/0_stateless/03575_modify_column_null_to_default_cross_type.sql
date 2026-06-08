-- Tags: no-random-settings, no-random-merge-tree-settings

-- use_variant_as_common_type routes ifNull through Variant types (#96790) and masks the
-- NO_COMMON_TYPE failure, so the test would pass on master without the fix. Disable it to
-- reproduce the bug this PR fixes (and to pass bugfix-validation).
SET use_variant_as_common_type = 0;

-- Case 1: Nullable(UInt8) -> String
DROP TABLE IF EXISTS nullable_cross_type_test;
CREATE TABLE nullable_cross_type_test (x Nullable(UInt8), y String) ORDER BY tuple();
INSERT INTO nullable_cross_type_test VALUES (NULL, 'a'), (42, 'b');
SELECT * FROM nullable_cross_type_test ORDER BY y;
ALTER TABLE nullable_cross_type_test MODIFY COLUMN x String DEFAULT '';
SELECT * FROM nullable_cross_type_test ORDER BY y;
DROP TABLE nullable_cross_type_test;

-- Case 2: Nullable(UInt8) -> LowCardinality(String)
-- Guards against the invalid Nullable(LowCardinality(String)) cast target.
DROP TABLE IF EXISTS nullable_cross_type_lc_test;
CREATE TABLE nullable_cross_type_lc_test (x Nullable(UInt8), y String) ORDER BY tuple();
INSERT INTO nullable_cross_type_lc_test VALUES (NULL, 'a'), (42, 'b');
ALTER TABLE nullable_cross_type_lc_test MODIFY COLUMN x LowCardinality(String) DEFAULT '';
SELECT * FROM nullable_cross_type_lc_test ORDER BY y;
DROP TABLE nullable_cross_type_lc_test;
