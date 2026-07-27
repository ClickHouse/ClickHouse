-- ALTER MODIFY COLUMN that turns a column *into* a type with dynamic subcolumns (Dynamic, JSON)
-- on a Wide part is handled by the partial-mutation path, which re-creates the new column's
-- data-dependent streams (variant_discr, SharedVariant, element streams). Stale-file accounting
-- in `collectFilesForRenames` compares state-less stream enumerations and cannot see those new
-- streams, so it flags the freshly written stream for removal from checksums.txt. Dropping it
-- leaves the file on disk but absent from checksums, and any later read/merge aborts with
-- "Stream y.variant_discr for column y with type Dynamic(...) is not found".
-- Regression coverage: the stale-file removal must keep any file the writer just produced.
-- See https://github.com/ClickHouse/ClickHouse/issues/107561

SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;
SET allow_experimental_json_type = 1;
SET use_variant_as_common_type = 1;

-- Case 1: Variant -> Dynamic. Source column has no dynamic subcolumns, target does.
-- Full part storage is pinned: a Packed part takes the full-rewrite path, which never reaches
-- the stale-file accounting this test covers.
DROP TABLE IF EXISTS t_modify_to_dyn;
CREATE TABLE t_modify_to_dyn (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY x
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
         min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

INSERT INTO t_modify_to_dyn SELECT number, number FROM numbers(3);
ALTER TABLE t_modify_to_dyn MODIFY COLUMN y Variant(UInt64, String) SETTINGS mutations_sync = 1;
INSERT INTO t_modify_to_dyn SELECT number, multiIf(number % 2 = 0, number, 'str_' || toString(number)) FROM numbers(3, 3);

ALTER TABLE t_modify_to_dyn MODIFY COLUMN y Dynamic(max_types = 18) SETTINGS mutations_sync = 1;
INSERT INTO t_modify_to_dyn SELECT number, number FROM numbers(6, 3);
INSERT INTO t_modify_to_dyn SELECT number, 'str_' || toString(number) FROM numbers(9, 3);

OPTIMIZE TABLE t_modify_to_dyn FINAL;

SELECT count(), countIf(y IS NOT NULL) FROM t_modify_to_dyn;
SELECT dynamicType(y) AS t, count() FROM t_modify_to_dyn GROUP BY t ORDER BY t;
SELECT x, y FROM t_modify_to_dyn ORDER BY x;
CHECK TABLE t_modify_to_dyn SETTINGS check_query_single_value_result = 1;

-- Checksums must be consistent after a reload (loads and validates checksums.txt).
DETACH TABLE t_modify_to_dyn;
ATTACH TABLE t_modify_to_dyn;
SELECT count() FROM t_modify_to_dyn;

DROP TABLE t_modify_to_dyn;

-- Case 2: a column declared Variant up front, then modified into Dynamic. This is the shape the
-- AST fuzzer produced in Stress test (arm_tsan) on PR #104510: the source column already has a
-- variant_discr stream, so the freshly written one collides by name with the stale-file accounting.
-- A plain String -> JSON conversion cannot cover this: it has no colliding stream name, so it
-- passes with or without the fix. Variant -> JSON is rejected outright (only String/Map/Object/
-- Tuple/JSON can be cast to JSON), so Dynamic is the only reachable colliding target here.
DROP TABLE IF EXISTS t_modify_variant_to_dyn;
CREATE TABLE t_modify_variant_to_dyn (x UInt64, y Variant(UInt64, String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
         min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

INSERT INTO t_modify_variant_to_dyn SELECT number, number FROM numbers(3);
INSERT INTO t_modify_variant_to_dyn SELECT number, 'str_' || toString(number) FROM numbers(3, 3);

ALTER TABLE t_modify_variant_to_dyn MODIFY COLUMN y Dynamic(max_types = 4) SETTINGS mutations_sync = 1;
INSERT INTO t_modify_variant_to_dyn SELECT number, number FROM numbers(6, 3);

OPTIMIZE TABLE t_modify_variant_to_dyn FINAL;

SELECT count() FROM t_modify_variant_to_dyn;
SELECT dynamicType(y) AS t, count() FROM t_modify_variant_to_dyn GROUP BY t ORDER BY t;
CHECK TABLE t_modify_variant_to_dyn SETTINGS check_query_single_value_result = 1;

-- The MODIFY must actually take the partial-mutation path; a full rewrite would satisfy every
-- assertion above while bypassing the stale-file accounting this test covers.
SYSTEM FLUSH LOGS part_log;
SELECT sum(ProfileEvents['MutationSomePartColumns']) > 0, sum(ProfileEvents['MutationAllPartColumns'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_modify_variant_to_dyn' AND event_type = 'MutatePart';

DETACH TABLE t_modify_variant_to_dyn;
ATTACH TABLE t_modify_variant_to_dyn;
SELECT count() FROM t_modify_variant_to_dyn;

DROP TABLE t_modify_variant_to_dyn;
