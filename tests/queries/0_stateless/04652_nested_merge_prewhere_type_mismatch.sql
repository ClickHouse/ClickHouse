-- Tags: no-old-analyzer
--       no-old-analyzer: reading a column absent from one `Merge` child (the het section's
--       `WHERE y != 0`) fills defaults only under the analyzer; the old one throws
--       UNKNOWN_IDENTIFIER. The explicit `SETTINGS enable_analyzer = 0` lines still cover it.

-- The transitive PREWHERE type-mismatch guards through local wrappers (Merge, MaterializedView,
-- Buffer, Alias). The row-policy / Remote / lazy-proxy / subcolumn carriers live in
-- 04652_nested_merge_prewhere_type_mismatch_2 - split so each half fits the flaky-check budget.

-- `StorageMerge::supportedPrewhereColumns` compares the root type only against each child's
-- *declared* columns. A nested `Merge` can declare a matching type while its own leaf differs, so
-- PREWHERE was admitted, built against the root type, then re-derived against the leaf's type -
-- `ActionsDAG` then aborted with `Unexpected return type from notEquals. Expected Nullable(UInt8).
-- Got UInt8`. The column must be rejected for PREWHERE transitively, like the single-level case.

DROP TABLE IF EXISTS t_leaf;
DROP TABLE IF EXISTS t_inner;
DROP TABLE IF EXISTS t_outer;

CREATE TABLE t_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_leaf SELECT number, number + 1 FROM numbers(10);

-- `x` is Nullable here but not in the leaf: the mismatch is one level below the outer table.
CREATE TABLE t_inner (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^t_leaf$');
CREATE TABLE t_outer (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^t_inner$');

SELECT '-- single level: the mismatched column is already rejected for PREWHERE --';
SELECT count() FROM t_inner PREWHERE x != 0; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- nested: must be rejected too, not abort in ActionsDAG --';
SELECT count() FROM t_outer PREWHERE x != 0; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a column whose type matches all the way down still supports PREWHERE --';
SELECT count() FROM t_outer PREWHERE y != 0;
-- Read the columns too: `count()` alone need not materialize them, so it would not exercise the
-- leaf's `UInt64` -> the root's `Nullable(UInt64)` conversion that the abort came from.
SELECT x, y FROM t_outer PREWHERE y != 0 ORDER BY x LIMIT 3;

SELECT '-- the same predicate as WHERE keeps working --';
SELECT count() FROM t_outer WHERE x != 0;
SELECT count() FROM t_outer WHERE y != 0;
SELECT x, y FROM t_outer WHERE x != 0 ORDER BY x LIMIT 3;

DROP TABLE t_outer;
DROP TABLE t_inner;
DROP TABLE t_leaf;

-- `StorageMaterializedView::supportedPrewhereColumns` had the same gap: it compared the view schema
-- against the target's *declared* columns only. A target that aggregates other tables itself hides
-- the mismatch one level further down, so the same abort was reachable without any `Merge` on top.

DROP TABLE IF EXISTS mv_src;
DROP TABLE IF EXISTS mv_dst;
DROP VIEW IF EXISTS mv_one;
DROP VIEW IF EXISTS mv_two;

CREATE TABLE mv_src (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mv_dst (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_one TO mv_dst AS SELECT CAST(x, 'Nullable(UInt64)') AS x, y FROM mv_src;
CREATE MATERIALIZED VIEW mv_two TO mv_one AS SELECT CAST(x, 'Nullable(UInt64)') AS x, y FROM mv_src;
INSERT INTO mv_dst SELECT number, number + 1 FROM numbers(10);

SELECT '-- view over a mismatched target is already rejected --';
SELECT x, y FROM mv_one PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- view over a view must be rejected too --';
SELECT x, y FROM mv_two PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a matching column still supports PREWHERE through the chain --';
SELECT count() FROM mv_two PREWHERE y != 0;
SELECT x, y FROM mv_two PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- the same predicates as WHERE keep working --';
SELECT count() FROM mv_two WHERE x != 0;
SELECT count() FROM mv_two WHERE y != 0;
SELECT x, y FROM mv_two WHERE x != 0 ORDER BY x LIMIT 3;

DROP VIEW mv_two;
DROP VIEW mv_one;
DROP TABLE mv_dst;
DROP TABLE mv_src;

-- A `Merge` whose child is a view chain: the outer `Merge` sees a matching declared type on the
-- view, so it can only reject `x` if the view itself reports transitively.

DROP TABLE IF EXISTS mvm_src;
DROP TABLE IF EXISTS mvm_dst;
DROP VIEW IF EXISTS mvm_view;
DROP TABLE IF EXISTS mvm_merge;

CREATE TABLE mvm_src (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mvm_dst (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mvm_view TO mvm_dst AS SELECT CAST(x, 'Nullable(UInt64)') AS x, y FROM mvm_src;
CREATE TABLE mvm_merge (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^mvm_view$');
INSERT INTO mvm_dst SELECT number, number + 1 FROM numbers(10);

SELECT '-- Merge over a materialized view must be rejected too --';
SELECT x, y FROM mvm_merge PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- and a matching column still works --';
SELECT count() FROM mvm_merge PREWHERE y != 0;
SELECT x, y FROM mvm_merge PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- the same predicates as WHERE keep working --';
SELECT count() FROM mvm_merge WHERE x != 0;
SELECT count() FROM mvm_merge WHERE y != 0;
SELECT x, y FROM mvm_merge WHERE x != 0 ORDER BY x LIMIT 3;

DROP TABLE mvm_merge;
DROP VIEW mvm_view;
DROP TABLE mvm_dst;
DROP TABLE mvm_src;

-- `StorageBuffer` forwards `supportsPrewhere()` to its destination but did not forward
-- `supportedPrewhereColumns()`, and its read() hands the already-built PREWHERE straight to the
-- destination (the Buffer and the Merge declare identical structures, so this is the
-- same-structure fast path). The same abort was reachable through the Buffer wrapper.

DROP TABLE IF EXISTS buf_leaf;
DROP TABLE IF EXISTS buf_merge;
DROP TABLE IF EXISTS buf_top;

CREATE TABLE buf_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO buf_leaf SELECT number, number + 1 FROM numbers(10);
CREATE TABLE buf_merge (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^buf_leaf$');
-- Flush thresholds far above anything this test does: all rows stay in buf_leaf, none in the buffer.
CREATE TABLE buf_top (x Nullable(UInt64), y UInt64)
    ENGINE = Buffer(currentDatabase(), buf_merge, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);

SELECT '-- Buffer over a Merge with a mismatched leaf must be rejected too --';
SELECT x, y FROM buf_top PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a matching column still supports PREWHERE through the Buffer --';
SELECT count() FROM buf_top PREWHERE y != 0;
SELECT x, y FROM buf_top PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- the same predicates as WHERE keep working --';
SELECT count() FROM buf_top WHERE x != 0;
SELECT count() FROM buf_top WHERE y != 0;
SELECT x, y FROM buf_top WHERE x != 0 ORDER BY x LIMIT 3;

-- The Buffer may also declare types that differ from the destination's own declaration (found by
-- the AST fuzzer). That alone is supported: read() prepends a converting prefix to the filter
-- (00910_buffer_prewhere_different_types). But the prefix used to convert the *whole* sample
-- block, so reading any other column whose destination declaration lies about the leaf executed
-- a bad cast inside the leaf. The prefix must only keep what the filter itself consumes.
CREATE TABLE buf_bad (x Decimal(18, 15), y Enum8('e1' = -127, 'v0' = 0))
    ENGINE = Buffer(currentDatabase(), buf_merge, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);
-- `y` matches the destination but `x` does not.
CREATE TABLE buf_partial (x Decimal(18, 15), y UInt64)
    ENGINE = Buffer(currentDatabase(), buf_merge, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);

SELECT '-- a column the destination rejects stays rejected, whatever the Buffer declares --';
SELECT y, x FROM buf_bad PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM buf_partial PREWHERE x != 0; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a Buffer-only type drift converts at read: PREWHERE fails like the WHERE twin, not with an abort --';
SELECT y, x FROM buf_bad PREWHERE y <= 1024 ORDER BY y LIMIT 3; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT y, x FROM buf_bad WHERE y <= 1024 ORDER BY y LIMIT 3; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

SELECT '-- reading a destination-drifted column next to a clean PREWHERE must not abort --';
SELECT count() FROM buf_partial PREWHERE y != 0;
SELECT x, y FROM buf_partial PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- a row policy is a filter too: same converting prefix, same pruning --';
CREATE ROW POLICY rp_04652 ON buf_partial FOR SELECT USING y != 0 TO CURRENT_USER;
SELECT x, y FROM buf_partial ORDER BY y LIMIT 3;
DROP ROW POLICY rp_04652 ON buf_partial;

SELECT '-- a policy on a destination-drifted column is filtered above the read, not pushed --';
CREATE ROW POLICY rp_04652_x ON buf_partial FOR SELECT USING x != 0 TO CURRENT_USER;
SELECT x, y FROM buf_partial ORDER BY y LIMIT 3;
DROP ROW POLICY rp_04652_x ON buf_partial;

DROP TABLE buf_partial;
DROP TABLE buf_bad;
DROP TABLE buf_top;
DROP TABLE buf_merge;
DROP TABLE buf_leaf;

-- The name existing in the destination is not enough either: the filter is forwarded into the raw
-- destination read, where an ALIAS twin is not a physical input (found by review). The kinds must
-- be compatible the same way StorageMerge requires.
CREATE TABLE buf_alias_dst (x UInt64, y UInt64 ALIAS x) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE buf_alias (y UInt64)
    ENGINE = Buffer(currentDatabase(), buf_alias_dst, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);
INSERT INTO buf_alias_dst (x) SELECT number FROM numbers(10);

SELECT '-- a destination ALIAS twin is rejected for PREWHERE, not forwarded --';
SELECT count() FROM buf_alias PREWHERE y != 0; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM buf_alias PREWHERE y != 0 SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_PREWHERE }
-- Reading it through the Buffer does not work regardless; the policy must not turn that into a push.
CREATE ROW POLICY rp_04652_alias ON buf_alias FOR SELECT USING y != 0 TO CURRENT_USER;
SELECT count() FROM buf_alias; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT count() FROM buf_alias SETTINGS enable_analyzer = 0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DROP ROW POLICY rp_04652_alias ON buf_alias;

SELECT '-- a MATERIALIZED twin is physical in the destination and stays supported --';
CREATE TABLE buf_mat_dst (x UInt64, y UInt64 MATERIALIZED x + 1) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE buf_mat (y UInt64)
    ENGINE = Buffer(currentDatabase(), buf_mat_dst, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);
INSERT INTO buf_mat_dst (x) SELECT number FROM numbers(10);
SELECT count() FROM buf_mat PREWHERE y > 5;
SELECT count() FROM buf_mat PREWHERE y > 5 SETTINGS enable_analyzer = 0;
CREATE ROW POLICY rp_04652_mat ON buf_mat FOR SELECT USING y > 5 TO CURRENT_USER;
SELECT count() FROM buf_mat;
DROP ROW POLICY rp_04652_mat ON buf_mat;

DROP TABLE buf_mat;
DROP TABLE buf_mat_dst;
DROP TABLE buf_alias;
DROP TABLE buf_alias_dst;

-- The same physicality rule holds through a materialized view: the target twin can drift to an
-- ALIAS after the view is created.
CREATE TABLE mv_alias_tgt (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_alias TO mv_alias_tgt (y UInt64) AS SELECT 1::UInt64 AS y;
INSERT INTO mv_alias_tgt (x, y) SELECT number, number FROM numbers(10);
ALTER TABLE mv_alias_tgt MODIFY COLUMN y UInt64 ALIAS x;

SELECT '-- a target ALIAS twin behind a view is rejected for PREWHERE too --';
SELECT count() FROM mv_alias PREWHERE y != 0; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM mv_alias PREWHERE y != 0 SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_PREWHERE }
CREATE ROW POLICY rp_04652_mv_alias ON mv_alias FOR SELECT USING y != 0 TO CURRENT_USER;
SELECT count() FROM mv_alias; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
SELECT count() FROM mv_alias SETTINGS enable_analyzer = 0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DROP ROW POLICY rp_04652_mv_alias ON mv_alias;

DROP VIEW mv_alias;
DROP TABLE mv_alias_tgt;

-- A column a child lacks entirely must fail closed too (found by review): the child read strips
-- it and fills defaults only afterwards, so a pushed filter has no input for it.
CREATE TABLE het_leaf1 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE het_leaf2 (x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE het_m (x UInt64, y UInt64) ENGINE = Merge(currentDatabase(), '^het_leaf[12]$');
INSERT INTO het_leaf1 SELECT number, number + 1 FROM numbers(5);
INSERT INTO het_leaf2 SELECT number + 100 FROM numbers(5);

SELECT '-- a column missing from one child is rejected for PREWHERE --';
SELECT count() FROM het_m PREWHERE y != 0; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM het_m PREWHERE y != 0 SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a column every child declares still supports PREWHERE, and WHERE sees the defaults --';
SELECT count() FROM het_m PREWHERE x >= 100;
SELECT count() FROM het_m WHERE y != 0;

SELECT '-- a policy on the missing column filters above the read instead of failing inside it --';
CREATE ROW POLICY rp_04652_het ON het_m FOR SELECT USING y != 0 TO CURRENT_USER;
SELECT count() FROM het_m;
SELECT count() FROM het_m SETTINGS enable_analyzer = 0;
DROP ROW POLICY rp_04652_het ON het_m;

SELECT '-- a policy on the shared column still pushes --';
CREATE ROW POLICY rp_04652_het_x ON het_m FOR SELECT USING x < 100 TO CURRENT_USER;
SELECT count() FROM het_m;
DROP ROW POLICY rp_04652_het_x ON het_m;

DROP TABLE het_m;
DROP TABLE het_leaf2;
DROP TABLE het_leaf1;

-- `StorageAlias` forwards the whole PREWHERE contract to its target, so an alias over a nested
-- `Merge` must reject the mismatched column exactly like the target itself does (transitively).

DROP TABLE IF EXISTS ali_leaf;
DROP TABLE IF EXISTS ali_inner;
DROP TABLE IF EXISTS ali_outer;
DROP TABLE IF EXISTS ali;

CREATE TABLE ali_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ali_leaf SELECT number, number + 1 FROM numbers(10);
CREATE TABLE ali_inner (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^ali_leaf$');
CREATE TABLE ali_outer (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^ali_inner$');
CREATE TABLE ali ENGINE = Alias(ali_outer);

SELECT '-- Alias over a nested Merge must be rejected too --';
SELECT x, y FROM ali PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a matching column still supports PREWHERE through the Alias --';
SELECT count() FROM ali PREWHERE y != 0;
SELECT x, y FROM ali PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- the same predicates as WHERE keep working --';
SELECT count() FROM ali WHERE x != 0;
SELECT count() FROM ali WHERE y != 0;
SELECT x, y FROM ali WHERE x != 0 ORDER BY x LIMIT 3;

DROP TABLE ali;
DROP TABLE ali_outer;
DROP TABLE ali_inner;
DROP TABLE ali_leaf;
