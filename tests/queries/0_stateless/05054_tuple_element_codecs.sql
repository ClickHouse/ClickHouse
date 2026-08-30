SET print_pretty_type_names = 0;

DROP TABLE IF EXISTS ts_series;
DROP TABLE IF EXISTS t_check;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_alter;
DROP TABLE IF EXISTS t_saf;
DROP TABLE IF EXISTS t_map;

SELECT 'Create and show:';

CREATE TABLE ts_series
(
    id UInt64,
    samples Array(Tuple(
        timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
        value Float64 CODEC(Gorilla, ZSTD(1))))
)
ENGINE = MergeTree ORDER BY id;

SHOW CREATE TABLE ts_series;

INSERT INTO ts_series VALUES (1, [('2026-08-28 10:00:00.000', 41.5), ('2026-08-28 10:00:15.000', 42.)]);
INSERT INTO ts_series VALUES (2, [('2026-08-28 11:00:00.000', 1.)]);

SELECT 'Data:';
SELECT id, samples FROM ts_series ORDER BY id;

OPTIMIZE TABLE ts_series FINAL;
SELECT 'After merge:';
SELECT id, samples FROM ts_series ORDER BY id;

DETACH TABLE ts_series;
ATTACH TABLE ts_series;
SELECT 'After DETACH and ATTACH:';
SHOW CREATE TABLE ts_series;
SELECT id, samples FROM ts_series ORDER BY id;

SELECT 'Codecs are applied per subcolumn:';

CREATE TABLE t_check
(
    v Array(Tuple(a UInt64 CODEC(ZSTD(1)), b UInt64)) CODEC(NONE)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_check SELECT [(0, number)] FROM numbers(100000);

SELECT name, compressed < (uncompressed / 10), compressed >= uncompressed
FROM system.parts_columns
ARRAY JOIN `subcolumns.names` AS name, `subcolumns.data_compressed_bytes` AS compressed, `subcolumns.data_uncompressed_bytes` AS uncompressed
WHERE database = currentDatabase() AND table = 't_check' AND column = 'v' AND active AND name IN ('a', 'b')
ORDER BY name;

SELECT 'Compact part:';

CREATE TABLE t_compact
(
    v Array(Tuple(a UInt64 CODEC(Delta, ZSTD(1)), b String))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_compact VALUES ([(1, 'x'), (2, 'y')]);
SELECT v FROM t_compact;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;

SELECT 'ALTER:';

CREATE TABLE t_alter (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_alter ADD COLUMN v Array(Tuple(a UInt64 CODEC(Delta, ZSTD(1)), b UInt64));
SHOW CREATE TABLE t_alter;
INSERT INTO t_alter VALUES (1, [(10, 20)]);
ALTER TABLE t_alter MODIFY COLUMN v Array(Tuple(a UInt64 CODEC(LZ4HC(5)), b UInt64 CODEC(ZSTD(3))));
SHOW CREATE TABLE t_alter;
ALTER TABLE t_alter MODIFY COLUMN v REMOVE CODEC;
SHOW CREATE TABLE t_alter;
SELECT x, v FROM t_alter;

SELECT 'SimpleAggregateFunction storage:';

CREATE TABLE t_saf
(
    id UInt64,
    t SimpleAggregateFunction(anyLast, Tuple(a UInt64 CODEC(Delta, ZSTD(1)), b Float64 CODEC(Gorilla, ZSTD(1))))
)
ENGINE = AggregatingMergeTree ORDER BY id;

SHOW CREATE TABLE t_saf;
INSERT INTO t_saf VALUES (1, (1, 1.5));
SELECT id, t FROM t_saf;

SELECT 'Tuple inside Map:';

CREATE TABLE t_map
(
    m Map(String, Tuple(x UInt64 CODEC(ZSTD(1)), y UInt64))
)
ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_map;
INSERT INTO t_map VALUES ({'k': (1, 2)});
SELECT m FROM t_map;

SELECT 'Errors:';

SELECT CAST((1, 2), 'Tuple(a UInt64 CODEC(LZ4), b UInt64)'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_err (t Tuple(x UInt32 CODEC(Gorilla, ZSTD(1)))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_err (x UInt64, a Tuple(p UInt64 CODEC(LZ4)) ALIAS tuple(x)) ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_err (n Nested(x UInt32 CODEC(LZ4))) ENGINE = MergeTree ORDER BY tuple(); -- { clientError SYNTAX_ERROR }
CREATE TABLE t_err (t Tuple(UInt32 CODEC(LZ4))) ENGINE = MergeTree ORDER BY tuple(); -- { clientError SYNTAX_ERROR }

DROP TABLE ts_series;
DROP TABLE t_check;
DROP TABLE t_compact;
DROP TABLE t_alter;
DROP TABLE t_saf;
DROP TABLE t_map;
