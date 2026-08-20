-- Tags: no-old-analyzer, no-replicated-database, shard
--       no-old-analyzer: this suite assumes the analyzer as the default, like its first half;
--       the explicit `SETTINGS enable_analyzer = 0` lines still cover the old one.
--       no-replicated-database: for the lazy_load_tables section below.
--       shard: the Remote-engine and Distributed sections need a second server address.

-- Continuation of 04652_nested_merge_prewhere_type_mismatch: the same transitive guards reached
-- through row policies, Distributed/Remote, lazy table proxies and subcolumns.

-- A refused row-level filter runs as a filter step right above the read, which only exists while
-- the storage stops at FetchColumns. A wrapper over Distributed processes past it and would skip
-- the policy silently (found by review), so it must fail closed instead.
CREATE TABLE rls_dist_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rls_dist_leaf SELECT number, number FROM numbers(10);
CREATE TABLE rls_dist (x UInt64, y UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), rls_dist_leaf);
CREATE MATERIALIZED VIEW rls_mv_dist TO rls_dist (x UInt64, y UInt32) AS SELECT x, y FROM rls_dist_leaf;

SELECT '-- a refused policy over a storage that processes past FetchColumns fails closed --';
SELECT count() FROM rls_mv_dist;
SELECT count() FROM rls_mv_dist PREWHERE y < 5; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM rls_mv_dist PREWHERE y < 5 SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_PREWHERE }
CREATE ROW POLICY rp_04652_dist ON rls_mv_dist FOR SELECT USING y < 5 TO CURRENT_USER;
SELECT count() FROM rls_mv_dist; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM rls_mv_dist SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_PREWHERE }
DROP ROW POLICY rp_04652_dist ON rls_mv_dist;

SELECT '-- so does a policy on a matching column: only the query text ships, never the filter --';
CREATE ROW POLICY rp_04652_dist_x ON rls_mv_dist FOR SELECT USING x < 5 TO CURRENT_USER;
SELECT count() FROM rls_mv_dist; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM rls_mv_dist SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_PREWHERE }
DROP ROW POLICY rp_04652_dist_x ON rls_mv_dist;

SELECT '-- a policy on the Distributed table itself fails closed too, instead of a silent drop --';
SELECT count() FROM rls_dist;
CREATE ROW POLICY rp_04652_dist_d ON rls_dist FOR SELECT USING y < 5 TO CURRENT_USER;
SELECT count() FROM rls_dist; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM rls_dist SETTINGS enable_analyzer = 0; -- { serverError ILLEGAL_PREWHERE }
DROP ROW POLICY rp_04652_dist_d ON rls_dist;

SELECT '-- the leaf policy still enforces through the Distributed read, matching shard-side model --';
CREATE ROW POLICY rp_04652_dist_l ON rls_dist_leaf FOR SELECT USING y < 5 TO CURRENT_USER;
-- Plan shipping (`serialize_query_plan = 1` + a non-local replica) skips the leaf's row policy
-- entirely - a pre-existing master bug this row is not about, see
-- https://github.com/ClickHouse/ClickHouse/issues/112891. Pin to text shipping.
SELECT count() FROM rls_dist SETTINGS serialize_query_plan = 0;
SELECT count() FROM rls_dist SETTINGS enable_analyzer = 0;
DROP ROW POLICY rp_04652_dist_l ON rls_dist_leaf;

DROP VIEW rls_mv_dist;
DROP TABLE rls_dist;
DROP TABLE rls_dist_leaf;

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

USE {CLICKHOUSE_DATABASE:Identifier};

-- A row policy is built against the outer schema like PREWHERE, but was pushed into read()
-- unconditionally, so it reached the differently-typed leaf and hit the same `Unexpected return
-- type` abort. The planner now pushes it only when every column it consumes is in the PREWHERE
-- contract; otherwise the policy filters above the read.

DROP TABLE IF EXISTS rp_leaf;
DROP TABLE IF EXISTS rp_inner;
DROP TABLE IF EXISTS rp_outer;

CREATE TABLE rp_leaf (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_leaf SELECT number, number + 1 FROM numbers(10);
CREATE TABLE rp_inner (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^rp_leaf$');
CREATE TABLE rp_outer (x Nullable(UInt64), y UInt64) ENGINE = Merge(currentDatabase(), '^rp_inner$');

SELECT '-- row policy on a mismatched column must not abort: single level --';
CREATE ROW POLICY rp_04652_single ON rp_inner FOR SELECT USING x != 0 TO CURRENT_USER;
SELECT x, y FROM rp_inner ORDER BY x LIMIT 3;
DROP ROW POLICY rp_04652_single ON rp_inner;

SELECT '-- and nested, under both analyzers --';
CREATE ROW POLICY rp_04652_nested ON rp_outer FOR SELECT USING x != 0 TO CURRENT_USER;
SELECT x, y FROM rp_outer ORDER BY x LIMIT 3;
SELECT x, y FROM rp_outer ORDER BY x LIMIT 3 SETTINGS enable_analyzer = 0;
DROP ROW POLICY rp_04652_nested ON rp_outer;

SELECT '-- a policy on a matching column keeps working --';
CREATE ROW POLICY rp_04652_match ON rp_outer FOR SELECT USING y > 3 TO CURRENT_USER;
SELECT x, y FROM rp_outer ORDER BY y LIMIT 3;
DROP ROW POLICY rp_04652_match ON rp_outer;

DROP TABLE rp_outer;
DROP TABLE rp_inner;
DROP TABLE rp_leaf;

-- The same carrier reaches a MaterializedView, which forwards it to the target untouched. The
-- guard must hold there too, including transitively through a view over a view.

DROP TABLE IF EXISTS rp_mv_src;
DROP TABLE IF EXISTS rp_mv_dst;
DROP VIEW IF EXISTS rp_mv_one;
DROP VIEW IF EXISTS rp_mv_two;

CREATE TABLE rp_mv_src (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE rp_mv_dst (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW rp_mv_one TO rp_mv_dst AS SELECT CAST(x, 'Nullable(UInt64)') AS x, y FROM rp_mv_src;
CREATE MATERIALIZED VIEW rp_mv_two TO rp_mv_one AS SELECT CAST(x, 'Nullable(UInt64)') AS x, y FROM rp_mv_src;
INSERT INTO rp_mv_dst SELECT number, number + 1 FROM numbers(10);

SELECT '-- row policy on a view whose type drifts from the target must not abort --';
CREATE ROW POLICY rp_04652_mv1 ON rp_mv_one FOR SELECT USING x != 0 TO CURRENT_USER;
SELECT x, y FROM rp_mv_one ORDER BY x LIMIT 3;
DROP ROW POLICY rp_04652_mv1 ON rp_mv_one;

SELECT '-- and through a view over a view, where the drift is one level down --';
CREATE ROW POLICY rp_04652_mv2 ON rp_mv_two FOR SELECT USING x != 0 TO CURRENT_USER;
SELECT x, y FROM rp_mv_two ORDER BY x LIMIT 3;
SELECT x, y FROM rp_mv_two ORDER BY x LIMIT 3 SETTINGS enable_analyzer = 0;
DROP ROW POLICY rp_04652_mv2 ON rp_mv_two;

DROP VIEW rp_mv_two;
DROP VIEW rp_mv_one;
DROP TABLE rp_mv_dst;
DROP TABLE rp_mv_src;

-- PREWHERE on a subcolumn is delegated through its origin column, and subcolumn sets (JSON
-- paths) are open-ended, so the contract lists origins only: `j.a` must be admitted whenever
-- `j` is (found by review: a closed NameSet of top-level names rejected every subcolumn).

DROP TABLE IF EXISTS sub_leaf;
DROP TABLE IF EXISTS sub_buf;
DROP TABLE IF EXISTS sub_merge;
DROP TABLE IF EXISTS sub_merge_bad;

SET enable_json_type = 1;
CREATE TABLE sub_leaf (j JSON, t Tuple(a UInt64), x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO sub_leaf SELECT '{"a":1}'::JSON, tuple(number), number FROM numbers(10);
CREATE TABLE sub_buf (j JSON, t Tuple(a UInt64), x UInt64)
    ENGINE = Buffer(currentDatabase(), sub_leaf, 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);
CREATE TABLE sub_merge (j JSON, t Tuple(a UInt64), x UInt64) ENGINE = Merge(currentDatabase(), '^sub_leaf$');
-- `t` deviates from the leaf here: its subcolumns must be rejected with it.
CREATE TABLE sub_merge_bad (t Tuple(a Nullable(UInt64)), x UInt64) ENGINE = Merge(currentDatabase(), '^sub_leaf$');

SELECT '-- a subcolumn PREWHERE rides its origin column through a Buffer --';
SELECT count() FROM sub_buf PREWHERE j.a = 1;
SELECT count() FROM sub_buf PREWHERE t.a < 5;
SELECT count() FROM sub_buf PREWHERE t.a < 5 SETTINGS enable_analyzer = 0;

SELECT '-- and through a Merge whose origin type matches the leaf --';
SELECT count() FROM sub_merge PREWHERE j.a = 1;
SELECT count() FROM sub_merge PREWHERE t.a < 5;

SELECT '-- the auto WHERE -> PREWHERE move admits subcolumns through their origin too --';
-- The AST-level optimizer consults the same contract; run it by disabling the plan-level one.
SELECT sum(x) FROM sub_buf WHERE t.a < 5 SETTINGS optimize_move_to_prewhere = 1, enable_analyzer = 0, query_plan_optimize_prewhere = 0;
SELECT sum(x) FROM sub_merge WHERE t.a < 5 SETTINGS optimize_move_to_prewhere = 1, enable_analyzer = 0, query_plan_optimize_prewhere = 0;
EXPLAIN SYNTAX SELECT sum(x) FROM sub_merge WHERE t.a < 5 SETTINGS optimize_move_to_prewhere = 1, enable_analyzer = 0, query_plan_optimize_prewhere = 0;

SELECT '-- a subcolumn of a type-drifted origin stays rejected --';
SELECT count() FROM sub_merge_bad PREWHERE t.a < 5; -- { serverError ILLEGAL_PREWHERE }
-- The auto-move refuses it too, keeping the filter above the read.
SELECT sum(x) FROM sub_merge_bad WHERE t.a < 5 SETTINGS optimize_move_to_prewhere = 1, enable_analyzer = 0, query_plan_optimize_prewhere = 0;
EXPLAIN SYNTAX SELECT sum(x) FROM sub_merge_bad WHERE t.a < 5 SETTINGS optimize_move_to_prewhere = 1, enable_analyzer = 0, query_plan_optimize_prewhere = 0;

SELECT '-- a row policy consuming a subcolumn maps to its origin for the push decision --';
CREATE ROW POLICY rp_04652_sub ON sub_buf FOR SELECT USING t.a < 3 TO CURRENT_USER;
SELECT x FROM sub_buf ORDER BY x LIMIT 5;
DROP ROW POLICY rp_04652_sub ON sub_buf;

DROP TABLE sub_merge_bad;
DROP TABLE sub_merge;
DROP TABLE sub_buf;
DROP TABLE sub_leaf;
