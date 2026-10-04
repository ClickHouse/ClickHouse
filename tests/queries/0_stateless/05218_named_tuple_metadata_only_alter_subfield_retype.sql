-- A metadata-only named Tuple ALTER may also retype a kept subfield when the new type has the same
-- on-disk representation (Date -> UInt16, enum widening). The same retyping without an added
-- subfield is not metadata-only and runs as a mutation.

DROP TABLE IF EXISTS t_named_tuple_retype;
SET allow_metadata_only_named_tuple_alter = 1;

DROP TABLE IF EXISTS t_named_tuple_retype;

CREATE TABLE t_named_tuple_retype (id UInt64, t Tuple(d Date, e Enum8('a' = 1, 'b' = 2), s String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_named_tuple_retype VALUES (1, ('2020-01-01', 'a', 'x')), (2, ('2020-01-02', 'b', 'y'));

SELECT 'retype of kept subfields plus an addition:';
ALTER TABLE t_named_tuple_retype MODIFY COLUMN t Tuple(d UInt16, e Enum8('a' = 1, 'b' = 2, 'c' = 3), s String, c Nullable(Int64));
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_retype';
SELECT t.d, t.e, t.s, t.c FROM t_named_tuple_retype ORDER BY id;
SELECT 't (whole):', t FROM t_named_tuple_retype ORDER BY id;

DROP TABLE t_named_tuple_retype;

CREATE TABLE t_named_tuple_retype (id UInt64, t Tuple(d Date, s String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_named_tuple_retype VALUES (1, ('2020-01-01', 'x'));

SELECT 'retype of a kept subfield without an addition:';
ALTER TABLE t_named_tuple_retype MODIFY COLUMN t Tuple(d UInt16, s String) SETTINGS alter_sync = 2;
SELECT 'mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_named_tuple_retype';
SELECT 't (whole):', t FROM t_named_tuple_retype ORDER BY id;

DROP TABLE t_named_tuple_retype;
