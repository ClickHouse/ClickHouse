-- Tags: no-parallel, no-random-merge-tree-settings

-- Regression test: merging parts after ALTER MODIFY COLUMN changed a column
-- from a non-Tuple type to a Tuple type used to crash with SIGSEGV because
-- SerializationInfoTuple::add did assert_cast on a plain SerializationInfo

DROP TABLE IF EXISTS t_alter_to_tuple;

CREATE TABLE t_alter_to_tuple (col String)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_alter_to_tuple VALUES ('(''hello'',''world'')');
INSERT INTO t_alter_to_tuple VALUES ('(''foo'',''bar'')');

-- Block mutations so the ALTER changes metadata but parts stay String-typed.
SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;

ALTER TABLE t_alter_to_tuple MODIFY COLUMN col Tuple(String, String) SETTINGS alter_sync = 0;
OPTIMIZE TABLE t_alter_to_tuple FINAL;

SELECT level, count() FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_alter_to_tuple'
  AND active
GROUP BY level;

SELECT col FROM t_alter_to_tuple ORDER BY col;

SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_max_part_size;

DROP TABLE t_alter_to_tuple;
