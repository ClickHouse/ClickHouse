-- Tags: no-old-analyzer
-- The `ALIAS`-over-virtual cases of issue #78465. Only the analyzer resolves an `ALIAS`
-- column defined over a virtual column, so the fixture cannot even be created under the old
-- analyzer. A session `SET allow_experimental_analyzer = 1` does not help: the mutation runs
-- in the background pool, whose context comes from `Context::getBackgroundContext` rather
-- than from the client session, so it always sees the server default. Hence the tag, which
-- the test runner resolves by probing the server's own setting.

DROP TABLE IF EXISTS t_mut_qp_alias;

SET mutations_sync = 2;

-- An ALIAS column is substituted by its defining expression after the mutation is analysed,
-- so an alias over one of these virtuals reaches the read path unless the check follows the
-- alias. Only `_table` and `_database` can be aliased at all: aliasing any part-derived
-- virtual (`_part`, `_sample_factor`, ...) is already rejected when the table is created.
CREATE TABLE t_mut_qp_alias (c0 UInt32, v UInt32, a_tbl String ALIAS _table, a_db String ALIAS _database, a_chain String ALIAS a_tbl, a_real UInt32 ALIAS c0 + 1)
ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_qp_alias SELECT number, 0 FROM numbers(4);
ALTER TABLE t_mut_qp_alias DELETE WHERE a_tbl != '' AND c0 < 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_mut_qp_alias DELETE WHERE a_db != '' AND c0 < 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- Also through a chain of aliases, and on the right-hand side of an UPDATE assignment.
ALTER TABLE t_mut_qp_alias DELETE WHERE a_chain != '' AND c0 < 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_mut_qp_alias UPDATE v = length(a_tbl) WHERE c0 < 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
-- An alias over a real column keeps working (a_real = c0 + 1, so this deletes c0 = 1 and 2).
ALTER TABLE t_mut_qp_alias DELETE WHERE a_real > 1 AND c0 < 3;
SELECT count() FROM t_mut_qp_alias;
DROP TABLE t_mut_qp_alias;

-- A Tuple subcolumn whose name collides with an ALIAS column is a real column access: it is
-- neither the virtual nor the alias, so following the alias must not reject it.
DROP TABLE IF EXISTS t_mut_qp_alias_subcol;
CREATE TABLE t_mut_qp_alias_subcol (c0 UInt32, tup Tuple(a_tbl String), a_tbl String ALIAS _table)
ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_qp_alias_subcol SELECT number, tuple('v') FROM numbers(4);
ALTER TABLE t_mut_qp_alias_subcol DELETE WHERE tup.a_tbl = 'missing' AND c0 < 2;
SELECT count() FROM t_mut_qp_alias_subcol;
-- The alias itself is still rejected on the same table.
ALTER TABLE t_mut_qp_alias_subcol DELETE WHERE a_tbl != '' AND c0 < 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DROP TABLE t_mut_qp_alias_subcol;

-- An alias may be named like one of these virtuals while being defined over another one.
-- The name must not shadow the virtual and let the reference reach the read path.
DROP TABLE IF EXISTS t_mut_qp_alias_shadow;
CREATE TABLE t_mut_qp_alias_shadow (c0 UInt32, `_table` String ALIAS _database)
ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_mut_qp_alias_shadow SELECT number FROM numbers(4);
ALTER TABLE t_mut_qp_alias_shadow DELETE WHERE `_table` != '' AND c0 < 2; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DROP TABLE t_mut_qp_alias_shadow;
