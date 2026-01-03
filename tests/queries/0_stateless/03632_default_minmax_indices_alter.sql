-- Test for issue #75677

drop table if exists t;

create table t (a UInt64, s String, dt DateTime64)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
  add_minmax_index_for_numeric_columns,
  add_minmax_index_for_string_columns,
  add_minmax_index_for_temporal_columns;

SHOW CREATE TABLE t;

ALTER TABLE t DROP COLUMN s;

SHOW CREATE TABLE t;

DROP TABLE t;