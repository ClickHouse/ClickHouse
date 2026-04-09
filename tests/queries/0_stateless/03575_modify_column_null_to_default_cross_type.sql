-- Tags: no-random-settings, no-random-merge-tree-settings

-- Test cross-type Nullable(X) -> Y conversion (e.g. Nullable(UInt8) -> String)
-- Follow-up to PR #84770
-- use_variant_as_common_type=0 to reproduce the bug even when Variant is available

SET use_variant_as_common_type = 0;

DROP TABLE IF EXISTS nullable_cross_type_test;
CREATE TABLE nullable_cross_type_test (x Nullable(UInt8), y String) ORDER BY tuple();
INSERT INTO nullable_cross_type_test VALUES (NULL, 'a'), (42, 'b');
SELECT * FROM nullable_cross_type_test ORDER BY y;
ALTER TABLE nullable_cross_type_test MODIFY COLUMN x String DEFAULT '';
SELECT * FROM nullable_cross_type_test ORDER BY y;
OPTIMIZE TABLE nullable_cross_type_test FINAL;
SELECT * FROM nullable_cross_type_test ORDER BY y;
DROP TABLE nullable_cross_type_test;
