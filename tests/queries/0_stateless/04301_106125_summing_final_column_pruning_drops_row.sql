-- Regression test for issue #106125:
-- SummingMergeTree FINAL silently drops a physically-present row when:
--   * read-in-order is engaged (ORDER BY primary key)
--   * column pruning narrows the read set to a subset of summing columns
--   * the only summing column read is 0 for that row
-- The full stored row has a non-zero value in a pruned column, so a real
-- merge keeps it. Before the fix, on-the-fly FINAL applied the zero-row-removal
-- rule to the pruned column subset and dropped the row.

DROP TABLE IF EXISTS s_106125;
DROP TABLE IF EXISTS s_106125_explicit;

-- Implicit columns_to_sum: SummingMergeTree picks all numeric non-key columns.
CREATE TABLE s_106125 (c0 UInt32, v_keep Int32, v_zero UInt32)
ENGINE = SummingMergeTree ORDER BY c0;

INSERT INTO s_106125 VALUES (0, 1, 0), (1, 1, 1), (2, 1, 2);
INSERT INTO s_106125 VALUES (3, 1, 3), (4, 1, 4), (5, 1, 5);

-- Reading only `c0` with a no-op filter on `v_zero` must return all 6 rows.
-- The c0=0 row stays because v_keep=1 (pruned) is non-zero in the full stored form.
SELECT 'implicit-projection-only-c0';
SELECT c0 FROM s_106125 FINAL WHERE v_zero >= 0 ORDER BY c0;

-- count() over the same subquery agrees with the materialized rows.
SELECT 'implicit-count-matches';
SELECT count() FROM (SELECT c0 FROM s_106125 FINAL WHERE v_zero >= 0 ORDER BY c0);

-- SELECT * always returned all 6 rows (full column visibility, never buggy).
SELECT 'implicit-select-star';
SELECT * FROM s_106125 FINAL WHERE v_zero >= 0 ORDER BY c0;

-- Disabling read-in-order avoided the bug pre-fix; must still return all 6 rows.
SELECT 'implicit-no-read-in-order';
SELECT c0 FROM s_106125 FINAL WHERE v_zero >= 0 ORDER BY c0
SETTINGS optimize_read_in_order = 0;

-- Projecting both columns also works (covers the pre-fix workaround).
SELECT 'implicit-c0-and-v_keep';
SELECT c0, v_keep FROM s_106125 FINAL ORDER BY c0;

-- Explicit columns_to_sum: SummingMergeTree(v_zero) sums only v_zero.
-- In this table the c0=0 row's only summing column is 0, so the row is
-- correctly dropped both before and after the fix (semantic difference,
-- not a bug). Kept here to document the intentional behaviour.
CREATE TABLE s_106125_explicit (c0 UInt32, v_keep Int32, v_zero UInt32, v_other UInt32)
ENGINE = SummingMergeTree(v_zero) ORDER BY c0;

INSERT INTO s_106125_explicit VALUES (0, 1, 0, 5), (1, 1, 1, 5), (2, 1, 2, 5);
INSERT INTO s_106125_explicit VALUES (3, 1, 3, 5), (4, 1, 4, 5), (5, 1, 5, 5);

SELECT 'explicit-drops-zero-row';
SELECT c0 FROM s_106125_explicit FINAL WHERE v_zero >= 0 ORDER BY c0;

SELECT 'explicit-select-star-drops-zero-row';
SELECT * FROM s_106125_explicit FINAL ORDER BY c0;

DROP TABLE s_106125;
DROP TABLE s_106125_explicit;

-- A MATERIALIZED non-key summing column participates in the merge just like an
-- ordinary one (the merge header is the table sample block, which includes
-- materialized columns). It must be kept in the read set under FINAL too, else
-- the c0=0 row is dropped on its visible v_zero=0 while v_keep is non-zero.
DROP TABLE IF EXISTS s_106125_mat;
CREATE TABLE s_106125_mat (c0 UInt32, v_keep Int32 MATERIALIZED 1, v_zero UInt32)
ENGINE = SummingMergeTree ORDER BY c0;

INSERT INTO s_106125_mat (c0, v_zero) VALUES (0, 0), (1, 1), (2, 2);
INSERT INTO s_106125_mat (c0, v_zero) VALUES (0, 0), (3, 3);

SELECT 'mat-projection-only-c0';
SELECT c0 FROM s_106125_mat FINAL WHERE v_zero >= 0 ORDER BY c0;

SELECT 'mat-count-matches';
SELECT count() FROM (SELECT c0 FROM s_106125_mat FINAL WHERE v_zero >= 0);

SELECT 'mat-select-star';
SELECT c0, v_keep, v_zero FROM s_106125_mat FINAL ORDER BY c0;

SELECT 'mat-no-read-in-order';
SELECT c0 FROM s_106125_mat FINAL WHERE v_zero >= 0 ORDER BY c0
SETTINGS optimize_read_in_order = 0;

DROP TABLE s_106125_mat;

-- Explicit columns_to_sum naming a Nested table: the entry is the prefix
-- (SomeMap), while the physical columns are SomeMap.ID / SomeMap.Num. Keeping
-- the literal prefix would request a non-existent column; the read set must
-- expand it to the physical subcolumns. FINAL projecting only the key must work.
SET allow_deprecated_syntax_for_merge_tree = 1;
DROP TABLE IF EXISTS s_106125_nested;
CREATE TABLE s_106125_nested (k UInt64, SomeMap Nested(ID UInt32, Num Int64))
ENGINE = SummingMergeTree((SomeMap)) ORDER BY k;

INSERT INTO s_106125_nested (k, `SomeMap.ID`, `SomeMap.Num`) VALUES (0, [1], [100]), (1, [1], [100]);
INSERT INTO s_106125_nested (k, `SomeMap.ID`, `SomeMap.Num`) VALUES (0, [2], [150]), (1, [1], [-100]);

SELECT 'nested-projection-only-k';
SELECT k FROM s_106125_nested FINAL ORDER BY k;

SELECT 'nested-count-matches';
SELECT count() FROM (SELECT k FROM s_106125_nested FINAL);

SELECT 'nested-select-star';
SELECT k, `SomeMap.ID`, `SomeMap.Num` FROM s_106125_nested FINAL ORDER BY k;

DROP TABLE s_106125_nested;

-- Explicit multi-column columns_to_sum: c0=0 has v_a=0 but v_b=7, so a real
-- merge keeps it. Projecting only c0 and filtering on v_a must not drop it.
DROP TABLE IF EXISTS s_106125_multi;
CREATE TABLE s_106125_multi (c0 UInt32, v_a UInt32, v_b UInt32)
ENGINE = SummingMergeTree((v_a, v_b)) ORDER BY c0;

INSERT INTO s_106125_multi VALUES (0, 0, 7), (1, 1, 0), (2, 2, 2);
INSERT INTO s_106125_multi VALUES (0, 0, 0), (3, 0, 0);

SELECT 'multi-projection-c0-filter-v_a';
SELECT c0 FROM s_106125_multi FINAL WHERE v_a >= 0 ORDER BY c0;

SELECT 'multi-select-star';
SELECT c0, v_a, v_b FROM s_106125_multi FINAL ORDER BY c0;

DROP TABLE s_106125_multi;

-- Explicit scalar columns_to_sum that ALSO has a `*Map` Nested column. defineColumns
-- discovers `*Map`-suffixed maps and sums them regardless of columns_to_sum (the scalar
-- columns_to_sum filter never applies to maps), so a real merge keeps a row whose scalar
-- sum is 0 as long as its map is present. The read set must therefore pin SomeMap.* even
-- though only `v` is named in columns_to_sum, else FINAL+pruning drops the k=0 row.
SET allow_deprecated_syntax_for_merge_tree = 1;
DROP TABLE IF EXISTS s_106125_sum_and_map;
CREATE TABLE s_106125_sum_and_map (k UInt64, v UInt64, SomeMap Nested(ID UInt32, Num Int64))
ENGINE = SummingMergeTree(v) ORDER BY k;

INSERT INTO s_106125_sum_and_map (k, v, `SomeMap.ID`, `SomeMap.Num`) VALUES (0, 0, [1], [100]), (1, 5, [2], [200]);
INSERT INTO s_106125_sum_and_map (k, v, `SomeMap.ID`, `SomeMap.Num`) VALUES (0, 0, [1], [50]), (2, 7, [3], [300]);

SELECT 'sum-and-map-projection-only-k';
SELECT k FROM s_106125_sum_and_map FINAL WHERE v >= 0 ORDER BY k;

SELECT 'sum-and-map-count-matches';
SELECT count() FROM (SELECT k FROM s_106125_sum_and_map FINAL WHERE v >= 0);

SELECT 'sum-and-map-select-star';
SELECT k, v, `SomeMap.ID`, `SomeMap.Num` FROM s_106125_sum_and_map FINAL ORDER BY k;

DROP TABLE s_106125_sum_and_map;

-- A non-summable String non-key column is NOT a summing column: defineColumns puts it in
-- column_numbers_not_to_aggregate, so it never keeps a zero-sum row and must NOT be pinned
-- into the FINAL read set. The k=0 row (v sums to 0) is correctly dropped here, and `payload`
-- is not read. Locks in that the fix mirrors defineColumns without over-keeping.
DROP TABLE IF EXISTS s_106125_payload;
CREATE TABLE s_106125_payload (k UInt64, payload String, v UInt64)
ENGINE = SummingMergeTree ORDER BY k;

INSERT INTO s_106125_payload VALUES (0, 'a', 0), (1, 'b', 1);
INSERT INTO s_106125_payload VALUES (0, 'c', 0), (2, 'd', 2);

SELECT 'payload-string-not-a-summing-col';
SELECT k FROM s_106125_payload FINAL WHERE v >= 0 ORDER BY k;

SELECT 'payload-select-star';
SELECT k, payload, v FROM s_106125_payload FINAL ORDER BY k;

DROP TABLE s_106125_payload;

-- allow_tuple_element_aggregation: defineColumns flattens Tuple columns (flattenTupleRecursive)
-- and sums the leaves, so a real merge keeps a row whose only non-zero value lives in a tuple
-- leaf. The read set walks unflattened physical columns and would see `metrics` as one opaque
-- Tuple and prune it; the read set must instead descend the tuple and pin the whole physical
-- column when any leaf is summable, else FINAL+pruning drops the k=0 row (metrics.b=1).
DROP TABLE IF EXISTS s_106125_tuple;
CREATE TABLE s_106125_tuple (k UInt64, metrics Tuple(a Int64, b Int64))
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO s_106125_tuple VALUES (0, (0, 1)), (1, (5, 5));
INSERT INTO s_106125_tuple VALUES (0, (0, 0)), (2, (7, 7));

SELECT 'tuple-projection-only-k';
SELECT k FROM s_106125_tuple FINAL WHERE metrics.a >= 0 ORDER BY k;

SELECT 'tuple-count-matches';
SELECT count() FROM (SELECT k FROM s_106125_tuple FINAL WHERE metrics.a >= 0);

SELECT 'tuple-select-star';
SELECT k, metrics FROM s_106125_tuple FINAL ORDER BY k;

DROP TABLE s_106125_tuple;

-- Nested tuple under allow_tuple_element_aggregation: leaves are expanded recursively, so a row
-- kept only by a deeply nested leaf (m.inner.d) must survive FINAL+pruning on the outer leaf m.a.
DROP TABLE IF EXISTS s_106125_tuple_nested;
CREATE TABLE s_106125_tuple_nested (k UInt64, m Tuple(a Int64, inner Tuple(c Int64, d Int64)))
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO s_106125_tuple_nested VALUES (0, (0, (0, 3))), (1, (1, (1, 1)));
INSERT INTO s_106125_tuple_nested VALUES (0, (0, (0, 0))), (2, (2, (2, 2)));

SELECT 'tuple-nested-projection-only-k';
SELECT k FROM s_106125_tuple_nested FINAL WHERE m.a >= 0 ORDER BY k;

SELECT 'tuple-nested-select-star';
SELECT k, m FROM s_106125_tuple_nested FINAL ORDER BY k;

DROP TABLE s_106125_tuple_nested;

-- Explicit columns_to_sum that names ONLY a scalar while a Tuple column is NOT listed: defineColumns
-- copies the unlisted tuple's leaves (last-value), so they cannot keep a zero-sum group. The k=0 row
-- (other sums to 0) is correctly dropped, and `metrics` must NOT be over-pinned. Locks in that the
-- tuple expansion respects the explicit columns_to_sum ancestor filter, matching defineColumns.
DROP TABLE IF EXISTS s_106125_tuple_excluded;
CREATE TABLE s_106125_tuple_excluded (k UInt64, other UInt64, metrics Tuple(a Int64, b Int64))
ENGINE = SummingMergeTree(other) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO s_106125_tuple_excluded VALUES (0, 0, (0, 1)), (1, 5, (5, 5));
INSERT INTO s_106125_tuple_excluded VALUES (0, 0, (0, 0)), (2, 7, (7, 7));

SELECT 'tuple-excluded-projection-only-k';
SELECT k FROM s_106125_tuple_excluded FINAL WHERE other >= 0 ORDER BY k;

SELECT 'tuple-excluded-select-star';
SELECT k, other, metrics FROM s_106125_tuple_excluded FINAL ORDER BY k;

DROP TABLE s_106125_tuple_excluded;

-- Tuple wrapping a `*Map` Nested group under allow_tuple_element_aggregation. defineColumns flattens
-- the tuple, then treats `metrics.ratesMap.ID` / `metrics.ratesMap.Value` as a summed map because the
-- synthesized parent `metrics.ratesMap` ends with `Map` AND is a real tuple ancestor of the leaves.
-- In implicit mode the map is summed, so a row whose only non-zero content is the map must survive
-- FINAL+pruning. The read set must descend the tuple and keep it (the whole physical column is read).
SET allow_deprecated_syntax_for_merge_tree = 1;
DROP TABLE IF EXISTS s_106125_tuple_map_implicit;
CREATE TABLE s_106125_tuple_map_implicit (k UInt64, v Int64, metrics Tuple(ratesMap Tuple(ID Array(UInt32), Value Array(Int64))))
ENGINE = SummingMergeTree ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO s_106125_tuple_map_implicit VALUES (0, 0, (([1], [100]))), (1, 5, (([2], [200])));
INSERT INTO s_106125_tuple_map_implicit VALUES (0, 0, (([1], [50]))), (2, 7, (([3], [300])));

-- k=0 has v=0 but its tuple map sums to a non-empty value, so a real merge keeps it.
SELECT 'tuple-map-implicit-projection-only-k';
SELECT k FROM s_106125_tuple_map_implicit FINAL WHERE v >= 0 ORDER BY k;

SELECT 'tuple-map-implicit-select-star';
SELECT k, v, metrics FROM s_106125_tuple_map_implicit FINAL ORDER BY k;

DROP TABLE s_106125_tuple_map_implicit;

-- Explicit columns_to_sum naming the tuple ancestor `metrics`: the map inside is summed (the leaf's
-- ancestor `metrics` is in the list), so the k=0 row is kept and the tuple must be in the read set.
DROP TABLE IF EXISTS s_106125_tuple_map_listed;
CREATE TABLE s_106125_tuple_map_listed (k UInt64, v Int64, metrics Tuple(ratesMap Tuple(ID Array(UInt32), Value Array(Int64))))
ENGINE = SummingMergeTree(metrics) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO s_106125_tuple_map_listed VALUES (0, 0, (([1], [100]))), (1, 5, (([2], [200])));
INSERT INTO s_106125_tuple_map_listed VALUES (0, 0, (([1], [50]))), (2, 7, (([3], [300])));

SELECT 'tuple-map-listed-projection-only-k';
SELECT k FROM s_106125_tuple_map_listed FINAL WHERE v >= 0 ORDER BY k;

SELECT 'tuple-map-listed-select-star';
SELECT k, v, metrics FROM s_106125_tuple_map_listed FINAL ORDER BY k;

DROP TABLE s_106125_tuple_map_listed;

-- Bot review (2026-06-12): explicit SummingMergeTree(other) with an UNLISTED tuple `*Map`. defineColumns
-- leaves `metrics.ratesMap.*` in column_numbers_not_to_aggregate because the explicit columns list names
-- only `other` (the map leaf has no listed ancestor), so the map is copied last-value and cannot keep a
-- zero-`other` group. A real merge therefore drops the k=0 row; on-the-fly FINAL must match (it does), and
-- the read set must NOT pin the unlisted tuple map. This locks in that the tuple `*Map` recursion mirrors
-- defineColumns' real-map-ancestor + explicit-list checks rather than keeping every `*Map`-suffixed leaf.
DROP TABLE IF EXISTS s_106125_tuple_map_unlisted;
CREATE TABLE s_106125_tuple_map_unlisted (k UInt64, other UInt64, metrics Tuple(ratesMap Tuple(ID Array(UInt32), Value Array(Int64))))
ENGINE = SummingMergeTree(other) ORDER BY k
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO s_106125_tuple_map_unlisted VALUES (0, 0, (([1], [100]))), (1, 5, (([2], [200])));
INSERT INTO s_106125_tuple_map_unlisted VALUES (0, 0, (([1], [50]))), (2, 7, (([3], [300])));

SELECT 'tuple-map-unlisted-projection-only-k';
SELECT k FROM s_106125_tuple_map_unlisted FINAL WHERE other >= 0 ORDER BY k;

SELECT 'tuple-map-unlisted-select-star';
SELECT k, other, metrics FROM s_106125_tuple_map_unlisted FINAL ORDER BY k;

DROP TABLE s_106125_tuple_map_unlisted;
