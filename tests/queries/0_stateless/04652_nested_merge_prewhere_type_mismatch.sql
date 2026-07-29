-- Tags: no-replicated-database, shard
--       no-replicated-database: for the lazy_load_tables section below.
--       shard: the Remote-engine section needs a second server address.

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

-- The Buffer may also declare types that differ from the destination's own declaration, not just
-- from a leaf's (found by the AST fuzzer). Forwarding the destination's supported *names* is not
-- enough then: the built PREWHERE would be re-derived against the destination's type. Every
-- mismatched column must be rejected no matter which level disagrees.
CREATE TABLE buf_bad (x Decimal(18, 15), y Enum8('e1' = -127, 'v0' = 0))
    ENGINE = Buffer(currentDatabase(), buf_merge, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);
-- `y` matches the destination but `x` does not.
CREATE TABLE buf_partial (x Decimal(18, 15), y UInt64)
    ENGINE = Buffer(currentDatabase(), buf_merge, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);

SELECT '-- a Buffer whose own types differ from the destination must be rejected too --';
SELECT y, x FROM buf_bad PREWHERE y <= 1024 ORDER BY y LIMIT 3; -- { serverError ILLEGAL_PREWHERE }
SELECT y, x FROM buf_bad PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM buf_partial PREWHERE x != 0; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- while its type-matching column keeps working --';
SELECT count() FROM buf_partial PREWHERE y != 0;

DROP TABLE buf_partial;
DROP TABLE buf_bad;
DROP TABLE buf_top;
DROP TABLE buf_merge;
DROP TABLE buf_leaf;

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

-- `ENGINE = Remote` builds a `Distributed` over an ad-hoc cluster, so the shard re-plans the query
-- from its text rather than receiving a built PREWHERE. That re-planning happens against the real
-- nested `Merge`, so the abort was reachable there too and the rejection must survive the round trip.

DROP TABLE IF EXISTS rem_leaf;
DROP TABLE IF EXISTS rem_inner;
DROP TABLE IF EXISTS rem_outer;
DROP TABLE IF EXISTS rem;

CREATE TABLE rem_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO rem_leaf SELECT number, number + 1 FROM numbers(10);
CREATE TABLE rem_inner (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^rem_leaf$');
CREATE TABLE rem_outer (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^rem_inner$');
-- No port: the address resolves to this server's own tcp_port, as the other Remote-engine tests do.
CREATE TABLE rem (x Nullable(UInt64), y UInt64) ENGINE = Remote('127.0.0.1', currentDatabase(), rem_outer);

SELECT '-- Remote over a nested Merge must be rejected too --';
SELECT x, y FROM rem PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a matching column still supports PREWHERE through Remote --';
SELECT count() FROM rem PREWHERE y != 0;
SELECT x, y FROM rem PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- the same predicates as WHERE keep working --';
SELECT count() FROM rem WHERE x != 0;
SELECT count() FROM rem WHERE y != 0;
SELECT x, y FROM rem WHERE x != 0 ORDER BY x LIMIT 3;

DROP TABLE rem;
DROP TABLE rem_outer;
DROP TABLE rem_inner;
DROP TABLE rem_leaf;

-- `Distributed` is the one storage whose `supportsPrewhere()` (true) and
-- `canMoveConditionsToPrewhere()` (false) disagree, and a `Merge` over one inherits the
-- disagreement. `readImpl` hands the analyzed query straight to the target, so a view must
-- forward the target's refusal to auto-move instead of falling back to the
-- `IStorage::canMoveConditionsToPrewhere == supportsPrewhere` default.
CREATE TABLE cm_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO cm_leaf SELECT number, number + 1 FROM numbers(10);
CREATE TABLE cm_dist (x UInt64, y UInt64) ENGINE = Remote('127.0.0.1', currentDatabase(), cm_leaf);
CREATE TABLE cm_merge (x UInt64, y UInt64) ENGINE = Merge(currentDatabase(), '^cm_dist$');
CREATE TABLE cm_src (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW cm_mv_dist TO cm_dist AS SELECT x, y FROM cm_src;
CREATE MATERIALIZED VIEW cm_mv_merge TO cm_merge AS SELECT x, y FROM cm_src;

SELECT '-- a view over a target that refuses the auto-move still answers correctly --';
SELECT count() FROM cm_mv_dist WHERE x != 0 SETTINGS optimize_move_to_prewhere = 1;
SELECT x, y FROM cm_mv_dist WHERE x != 0 ORDER BY x LIMIT 3 SETTINGS optimize_move_to_prewhere = 1;

SELECT '-- and so does a view over a Merge that inherits the refusal --';
SELECT count() FROM cm_mv_merge WHERE x != 0 SETTINGS optimize_move_to_prewhere = 1;
SELECT x, y FROM cm_mv_merge WHERE x != 0 ORDER BY x LIMIT 3 SETTINGS optimize_move_to_prewhere = 1;

SELECT '-- an explicit PREWHERE through both views keeps working --';
SELECT count() FROM cm_mv_dist PREWHERE y != 0;
SELECT count() FROM cm_mv_merge PREWHERE y != 0;

DROP TABLE cm_mv_merge;
DROP TABLE cm_mv_dist;
DROP TABLE cm_src;
DROP TABLE cm_merge;
DROP TABLE cm_dist;
DROP TABLE cm_leaf;

-- With `lazy_load_tables = 1`, a re-attached table is a `StorageTableProxy` wrapping the real
-- storage. `StorageProxy` forwards `supportsPrewhere()` but did not forward `supportedPrewhereColumns()`
-- (default: `std::nullopt`, meaning "everything supported") nor `canMoveConditionsToPrewhere()`, so a
-- lazily-loaded `outer` here would admit `PREWHERE x != 0` unrestricted and abort in `ActionsDAG`,
-- even though the same table is correctly rejected before the DETACH/ATTACH round trip.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.lazy_leaf SELECT number, number + 1 FROM numbers(10);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_inner (x Nullable(UInt64), y UInt64)
    ENGINE = Merge({CLICKHOUSE_DATABASE_1:Identifier}, '^lazy_leaf$');
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_outer (x Nullable(UInt64), y UInt64)
    ENGINE = Merge({CLICKHOUSE_DATABASE_1:Identifier}, '^lazy_inner$');

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- Switch into the lazy database so the system.tables lookup below can filter on currentDatabase(),
-- as the style check requires (a `{...:String}` parameter is not recognized by it).
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT '-- re-attached tables are lazy proxies --';
SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;

SELECT '-- the proxy must still reject the mismatched column, not abort --';
SELECT x, y FROM lazy_outer PREWHERE x != 0 ORDER BY x LIMIT 3; -- { serverError ILLEGAL_PREWHERE }

SELECT '-- a matching column still supports PREWHERE through the proxy --';
SELECT count() FROM lazy_outer PREWHERE y != 0;
SELECT x, y FROM lazy_outer PREWHERE y != 0 ORDER BY y LIMIT 3;

SELECT '-- the same predicates as WHERE keep working through the proxy --';
SELECT count() FROM lazy_outer WHERE x != 0;
SELECT count() FROM lazy_outer WHERE y != 0;
SELECT x, y FROM lazy_outer WHERE x != 0 ORDER BY x LIMIT 3;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
