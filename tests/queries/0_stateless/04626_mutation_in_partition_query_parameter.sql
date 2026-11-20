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

-- `DateTime64` and `Time64` query parameters are substituted as a cast of a cast of a literal.

DROP TABLE IF EXISTS t_mutation_param_dt64;
CREATE TABLE t_mutation_param_dt64 (ts DateTime64(3, 'UTC'), k UInt8, v UInt8)
ENGINE = MergeTree PARTITION BY dateTrunc('day', ts) ORDER BY k;
INSERT INTO t_mutation_param_dt64 VALUES ('2024-01-01 10:00:00', 1, 0), ('2024-01-02 10:00:00', 2, 0), ('2024-01-03 10:00:00', 3, 0);

SET param_p2 = '2024-01-02 00:00:00', param_p3 = '2024-01-03 00:00:00';
ALTER TABLE t_mutation_param_dt64 DROP PARTITION {p2:DateTime64(0, 'UTC')};
ALTER TABLE t_mutation_param_dt64 UPDATE v = 1 IN PARTITION {p3:DateTime64(0, 'UTC')} WHERE 1 SETTINGS mutations_sync = 2;
SELECT k, v FROM t_mutation_param_dt64 ORDER BY k;
DETACH TABLE t_mutation_param_dt64;
ATTACH TABLE t_mutation_param_dt64;
SELECT k, v FROM t_mutation_param_dt64 ORDER BY k;
DROP TABLE t_mutation_param_dt64;

DROP TABLE IF EXISTS t_mutation_param_dt64_tuple;
CREATE TABLE t_mutation_param_dt64_tuple (ts DateTime64(3, 'UTC'), s String, k UInt8, v UInt8)
ENGINE = MergeTree PARTITION BY (dateTrunc('day', ts), s) ORDER BY k;
INSERT INTO t_mutation_param_dt64_tuple VALUES ('2024-01-01 10:00:00', 'a', 1, 0), ('2024-01-02 10:00:00', 'b', 2, 0), ('2024-01-03 10:00:00', 'c', 3, 0);

SET param_pt2 = '(''2024-01-02 00:00:00'',''b'')', param_pt3 = '(''2024-01-03 00:00:00'',''c'')';
ALTER TABLE t_mutation_param_dt64_tuple DROP PARTITION {pt2:Tuple(DateTime64(0, 'UTC'), String)};
ALTER TABLE t_mutation_param_dt64_tuple UPDATE v = 1 IN PARTITION {pt3:Tuple(DateTime64(0, 'UTC'), String)} WHERE 1 SETTINGS mutations_sync = 2;
SELECT k, v FROM t_mutation_param_dt64_tuple ORDER BY k;
DETACH TABLE t_mutation_param_dt64_tuple;
ATTACH TABLE t_mutation_param_dt64_tuple;
SELECT k, v FROM t_mutation_param_dt64_tuple ORDER BY k;
DROP TABLE t_mutation_param_dt64_tuple;

DROP TABLE IF EXISTS t_mutation_param_time64;
CREATE TABLE t_mutation_param_time64 (t Time64(3), k UInt8, v UInt8)
ENGINE = MergeTree PARTITION BY t ORDER BY k;
INSERT INTO t_mutation_param_time64 VALUES ('10:00:00', 1, 0), ('11:00:00', 2, 0), ('12:00:00.5', 3, 0);

SET param_t2 = '11:00:00', param_t3 = '12:00:00.5';
ALTER TABLE t_mutation_param_time64 DROP PARTITION {t2:Time64(3)};
ALTER TABLE t_mutation_param_time64 UPDATE v = 1 IN PARTITION {t3:Time64(3)} WHERE 1 SETTINGS mutations_sync = 2;
SELECT k, v FROM t_mutation_param_time64 ORDER BY k;
DETACH TABLE t_mutation_param_time64;
ATTACH TABLE t_mutation_param_time64;
SELECT k, v FROM t_mutation_param_time64 ORDER BY k;
DROP TABLE t_mutation_param_time64;
