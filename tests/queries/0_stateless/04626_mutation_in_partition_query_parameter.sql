-- Query parameter substitution rewrites `{param:Type}` into `_CAST(value, 'Type')`, and the
-- serialized mutation entry must be parseable back when it is re-read from disk on table load.

DROP TABLE IF EXISTS t_mutation_param;

CREATE TABLE t_mutation_param (id UInt64, d Date, flag Nullable(Bool))
ENGINE = MergeTree PARTITION BY toYYYYMMDD(d) ORDER BY id;

INSERT INTO t_mutation_param SELECT number, '2026-06-24', NULL FROM numbers(10);
INSERT INTO t_mutation_param SELECT number + 100, '2026-06-25', NULL FROM numbers(10);

SET param_day = '20260624';
SET param_date = '2026-06-25';

ALTER TABLE t_mutation_param UPDATE flag = true IN PARTITION {day:UInt32} WHERE toYYYYMMDD(d) = {day:UInt32} SETTINGS mutations_sync = 2;

SELECT countIf(flag), count() FROM t_mutation_param;

-- The partition can also be specified as an explicitly written cast of a literal.
ALTER TABLE t_mutation_param UPDATE flag = false IN PARTITION _CAST(20260624, 'UInt32') WHERE 1 SETTINGS mutations_sync = 2;

-- Arbitrary expressions are still not allowed in the partition.
ALTER TABLE t_mutation_param UPDATE flag = true IN PARTITION toYYYYMMDD({date:Date}) WHERE 1 SETTINGS mutations_sync = 2; -- { clientError SYNTAX_ERROR }

SELECT countIf(flag), count() FROM t_mutation_param;

-- Make sure the mutation entries written to disk can be parsed back on table load.
DETACH TABLE t_mutation_param;
ATTACH TABLE t_mutation_param;

SELECT countIf(flag), count() FROM t_mutation_param;

ALTER TABLE t_mutation_param DROP PARTITION {day:UInt32};

SELECT count() FROM t_mutation_param;

DROP TABLE t_mutation_param;

-- Tuple-typed query parameters for multi-, one- and zero-field partition keys.

DROP TABLE IF EXISTS t_mutation_param_tuple2;
CREATE TABLE t_mutation_param_tuple2 (id UInt64, a UInt32, s String, flag Nullable(Bool))
ENGINE = MergeTree PARTITION BY (a, s) ORDER BY id;
INSERT INTO t_mutation_param_tuple2 SELECT number, 1, 'x', NULL FROM numbers(5);
INSERT INTO t_mutation_param_tuple2 SELECT number + 100, 2, 'y', NULL FROM numbers(5);

SET param_part_two = '(1,''x'')';
ALTER TABLE t_mutation_param_tuple2 UPDATE flag = true IN PARTITION {part_two:Tuple(UInt32, String)} WHERE 1 SETTINGS mutations_sync = 2;
SELECT countIf(flag), count() FROM t_mutation_param_tuple2;
DETACH TABLE t_mutation_param_tuple2;
ATTACH TABLE t_mutation_param_tuple2;
SELECT countIf(flag), count() FROM t_mutation_param_tuple2;
DROP TABLE t_mutation_param_tuple2;

DROP TABLE IF EXISTS t_mutation_param_tuple1;
CREATE TABLE t_mutation_param_tuple1 (id UInt64, a UInt32, flag Nullable(Bool))
ENGINE = MergeTree PARTITION BY a ORDER BY id;
INSERT INTO t_mutation_param_tuple1 SELECT number, 1, NULL FROM numbers(5);
INSERT INTO t_mutation_param_tuple1 SELECT number + 100, 2, NULL FROM numbers(5);

SET param_part_one = '(1)';
ALTER TABLE t_mutation_param_tuple1 UPDATE flag = true IN PARTITION {part_one:Tuple(UInt32)} WHERE 1 SETTINGS mutations_sync = 2;
SELECT countIf(flag), count() FROM t_mutation_param_tuple1;
DETACH TABLE t_mutation_param_tuple1;
ATTACH TABLE t_mutation_param_tuple1;
SELECT countIf(flag), count() FROM t_mutation_param_tuple1;
DROP TABLE t_mutation_param_tuple1;

DROP TABLE IF EXISTS t_mutation_param_tuple0;
CREATE TABLE t_mutation_param_tuple0 (id UInt64, flag Nullable(Bool))
ENGINE = MergeTree PARTITION BY tuple() ORDER BY id;
INSERT INTO t_mutation_param_tuple0 SELECT number, NULL FROM numbers(5);

SET param_part_zero = '()';
ALTER TABLE t_mutation_param_tuple0 UPDATE flag = true IN PARTITION {part_zero:Tuple()} WHERE 1 SETTINGS mutations_sync = 2;
SELECT countIf(flag), count() FROM t_mutation_param_tuple0;
DETACH TABLE t_mutation_param_tuple0;
ATTACH TABLE t_mutation_param_tuple0;
SELECT countIf(flag), count() FROM t_mutation_param_tuple0;
DROP TABLE t_mutation_param_tuple0;
