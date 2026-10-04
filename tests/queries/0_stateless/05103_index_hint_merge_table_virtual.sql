-- https://github.com/ClickHouse/ClickHouse/issues/114013
-- `indexHint` contributes a second input for the same virtual column, and the expression that decides
-- which tables a `Merge` engine must read was executed over a block holding each virtual column once,
-- so the query failed with `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP TABLE IF EXISTS t_merge_hint_1;
DROP TABLE IF EXISTS t_merge_hint_2;
DROP TABLE IF EXISTS t_merge_hint_all;
CREATE TABLE t_merge_hint_1 (key UInt32, value UInt32) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_merge_hint_2 (key UInt32, value UInt32) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_merge_hint_1 VALUES (1, 10);
INSERT INTO t_merge_hint_2 VALUES (2, 20);
CREATE TABLE t_merge_hint_all (key UInt32, value UInt32) ENGINE = Merge(currentDatabase(), '^t_merge_hint_[0-9]$');

SELECT _table, key FROM t_merge_hint_all WHERE _table = 't_merge_hint_1' ORDER BY key;
SELECT _table, key FROM t_merge_hint_all WHERE indexHint(_table = 't_merge_hint_1') AND (_table = 't_merge_hint_1') ORDER BY key;
SELECT _table, key FROM t_merge_hint_all WHERE indexHint(_table = 't_merge_hint_1') ORDER BY key;
SELECT _table, key FROM t_merge_hint_all WHERE indexHint(key = 1) AND (_table = 't_merge_hint_1') ORDER BY key;
SELECT _table, key FROM t_merge_hint_all WHERE indexHint(_database = currentDatabase()) AND (_table = 't_merge_hint_2') ORDER BY key;
SELECT count() FROM t_merge_hint_all WHERE indexHint(_table = 't_merge_hint_1') AND (_table = 't_merge_hint_1');
SELECT count() FROM t_merge_hint_all WHERE indexHint(_table IN ('t_merge_hint_1', 't_merge_hint_2'));
SELECT _table, key FROM t_merge_hint_all WHERE indexHint(_table != 't_merge_hint_1') AND (_table != 't_merge_hint_1') ORDER BY key;

SELECT 'constant-only indexHint arguments do not lose the one-row block';
-- The split predicate can need no virtual column at all. It is still evaluated over the one-row block
-- that decides table selection, so it must keep reporting the value of the constant, not the value an
-- expression evaluated over zero rows would produce.
SELECT count() FROM t_merge_hint_all WHERE indexHint(materialize(1));
SELECT count() FROM t_merge_hint_all WHERE indexHint(materialize(1)) AND (_table = 't_merge_hint_1');
SELECT count() FROM t_merge_hint_all WHERE indexHint(materialize(1)) AND key >= 1;
SELECT _table, key FROM t_merge_hint_all WHERE indexHint(materialize(1)) ORDER BY key;
SELECT count() FROM t_merge_hint_all WHERE indexHint(1);
-- A constant-false hint prunes every table, as it does without this fix.
SELECT count() FROM t_merge_hint_all WHERE indexHint(materialize(0));

SELECT 'plain virtual column filters are unaffected';
SELECT count() FROM t_merge_hint_all WHERE _table = 't_merge_hint_2';
SELECT count() FROM t_merge_hint_all WHERE _database = currentDatabase();
SELECT count() FROM t_merge_hint_all;

DROP TABLE t_merge_hint_all;
DROP TABLE t_merge_hint_1;
DROP TABLE t_merge_hint_2;
