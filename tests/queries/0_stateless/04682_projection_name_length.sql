-- The projection name is used raw as a path component, and the code derives longer names from it
-- (`<name>.proj`, `<name>.tmp_proj`, `<name>_<block_num>.tmp_proj`, `delete_tmp_<name>_<n>.tmp_proj`).
-- Without a DDL check, a name that does not leave room for those is accepted and then every INSERT,
-- mutation and MATERIALIZE PROJECTION fails with an untyped Code 1001 naming nothing.
-- 214 is the current limit; 215 is one over.

DROP TABLE IF EXISTS t_at_limit;
DROP TABLE IF EXISTS t_over_limit;
DROP TABLE IF EXISTS t_gap;
DROP TABLE IF EXISTS t_delete_tmp_gap;
DROP TABLE IF EXISTS t_alter;
DROP TABLE IF EXISTS t_escaped;
DROP TABLE IF EXISTS t_short;
DROP TABLE IF EXISTS t_clone_source;
DROP TABLE IF EXISTS t_clone;
DROP TABLE IF EXISTS t_fixed;

SELECT '-- a name at the limit is accepted and works end to end';
CREATE TABLE t_at_limit (a UInt64, b UInt64,
    PROJECTION pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp
    (SELECT a, sum(b) GROUP BY a))
ENGINE = MergeTree ORDER BY a;
-- Two parts plus a mutation is what reaches the numbered `<name>_<block_num>.tmp_proj` form.
INSERT INTO t_at_limit SELECT number, number FROM numbers(5);
INSERT INTO t_at_limit SELECT number + 100, number FROM numbers(5);
ALTER TABLE t_at_limit UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_at_limit MATERIALIZE PROJECTION pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_at_limit FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT count() FROM t_at_limit;
-- Read the projection itself, not only the base table: a base-table count would pass even if the
-- projection had been silently skipped. Sum the rows rather than counting parts, which a background
-- merge may change; the two inserts use disjoint keys, so the aggregated row count does not.
SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_at_limit' AND active;

SELECT '-- one character over the limit is rejected at CREATE';
CREATE TABLE t_over_limit (a UInt64,
    PROJECTION ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp
    (SELECT a ORDER BY a))
ENGINE = MergeTree ORDER BY a; -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- a name in the gap the reporter measured (245, plain INSERT works, mutations do not) is rejected too';
CREATE TABLE t_gap (a UInt64,
    PROJECTION ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg
    (SELECT a ORDER BY a))
ENGINE = MergeTree ORDER BY a; -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- a name in the delete_tmp_ gap (240) is rejected as well';
CREATE TABLE t_delete_tmp_gap (a UInt64,
    PROJECTION dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
    (SELECT a ORDER BY a))
ENGINE = MergeTree ORDER BY a; -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- ALTER ADD PROJECTION is a separate DDL entry point';
CREATE TABLE t_alter (a UInt64) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_alter ADD PROJECTION aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (SELECT a ORDER BY a);
ALTER TABLE t_alter ADD PROJECTION aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (SELECT a ORDER BY a); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 't_alter';

SELECT '-- an already present name is add-time business: IF NOT EXISTS stays a no-op, a duplicate still reports ILLEGAL_PROJECTION';
-- The length check must not pre-empt either answer, otherwise a legacy over-limit projection could no
-- longer be re-declared as a no-op. Uses the 214 name added above, which is already present.
ALTER TABLE t_alter ADD PROJECTION IF NOT EXISTS aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (SELECT a ORDER BY a);
ALTER TABLE t_alter ADD PROJECTION aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (SELECT a ORDER BY a); -- { serverError ILLEGAL_PROJECTION }
SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 't_alter';

SELECT '-- the limit is on the raw name, not the escaped one: 214 raw with 40 dashes must be accepted';
-- escapeForFileName would expand each dash to three characters (294 > 214), but the projection name
-- reaches the filesystem unescaped, so an escaped-length bound would wrongly reject this.
CREATE TABLE t_escaped (a UInt64, b UInt64,
    PROJECTION `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx----------------------------------------yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy`
    (SELECT a, sum(b) GROUP BY a))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_escaped SELECT number, number FROM numbers(5);
INSERT INTO t_escaped SELECT number + 100, number FROM numbers(5);
ALTER TABLE t_escaped UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM t_escaped;
SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_escaped' AND active;

-- Must not regress: cloning a source whose projection name is within the limit keeps working.
SELECT '-- CREATE TABLE AS keeps working for a source within the limit';
CREATE TABLE t_clone_source (a UInt64,
    PROJECTION ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
    (SELECT a ORDER BY a))
ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_clone AS t_clone_source;
SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 't_clone';

-- The full-definition ATTACH entry point needs a runtime-generated UUID to stay parallel-safe, which
-- a .sql file cannot interpolate, so it lives in the sibling 04682_projection_name_length_attach.sh.

-- A table with index_granularity_bytes = 0 cannot use adaptive granularity, and then a projection
-- part may only be Wide: both MergeTreeDataPartBuilder::build and MarkType reject any other type.
-- A must-not-regress control: a within-limit projection on that shape still materializes as Wide
-- and answers correctly.
SELECT '-- a table with fixed granularity is unaffected';
CREATE TABLE t_fixed (a UInt64, b UInt64, PROJECTION p (SELECT a, sum(b) GROUP BY a))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity_bytes = 0;
INSERT INTO t_fixed SELECT number % 3, number FROM numbers(30);
ALTER TABLE t_fixed MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
-- The projection aggregates, so it holds one row per distinct key, not one per input row. Assert the
-- part type too: without adaptive granularity it must be Wide.
SELECT sum(rows), groupUniqArray(part_type) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_fixed' AND active;
SELECT sum(b), uniqExact(a) FROM t_fixed;

SELECT '-- an ordinary projection is unaffected';
CREATE TABLE t_short (a UInt64, b UInt64, PROJECTION p (SELECT a, sum(b) GROUP BY a)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_short SELECT number % 3, number FROM numbers(30);
INSERT INTO t_short SELECT number % 3, number FROM numbers(30);
OPTIMIZE TABLE t_short FINAL SETTINGS optimize_throw_if_noop = 1;
ALTER TABLE t_short MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT a, sum(b) FROM t_short GROUP BY a ORDER BY a;

DROP TABLE t_at_limit;
DROP TABLE t_alter;
DROP TABLE t_escaped;
DROP TABLE t_clone_source;
DROP TABLE t_clone;
DROP TABLE t_fixed;
DROP TABLE t_short;
