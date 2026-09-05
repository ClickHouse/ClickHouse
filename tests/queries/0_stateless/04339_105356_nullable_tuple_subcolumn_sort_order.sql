-- Tags: no-ordinary-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105356
-- Merge-time check: a Nullable subcolumn of Nullable(Tuple(...)) used as a sort key must keep the
-- outer null map, otherwise OPTIMIZE FINAL aborts with `Sort order of blocks violated`.

SET allow_experimental_nullable_tuple_type = 1;

-- Carrier 1: inner element `Nullable(Decimal)` (extracted as a ColumnNullable).
DROP TABLE IF EXISTS t_105356;

CREATE TABLE t_105356
(
    c0 Nullable(Tuple(String, Nullable(Decimal))),
    c2 Decimal
)
ENGINE = SummingMergeTree()
ORDER BY (`c0.2`, c2)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_105356 SELECT if(number = 0, NULL, ('a', toDecimal32(10, 0))), toDecimal32(number, 0) FROM numbers(2);
INSERT INTO t_105356 SELECT if(number = 0, NULL, ('b', toDecimal32(20, 0))), toDecimal32(number + 5, 0) FROM numbers(2);

OPTIMIZE TABLE t_105356 FINAL;

SELECT count() > 0, uniqExact(_part) FROM t_105356;
SELECT countIf(c0 IS NULL AND isNotNull(`c0.2`)) FROM t_105356;

DROP TABLE t_105356;

-- Carrier 2: inner element `LowCardinality(Nullable(String))` (nullability carried in the dictionary,
-- so it reaches the subcolumn creator as a plain ColumnLowCardinality).
DROP TABLE IF EXISTS t_105356_lc;

CREATE TABLE t_105356_lc
(
    c0 Nullable(Tuple(String, LowCardinality(Nullable(String)))),
    c2 Decimal
)
ENGINE = SummingMergeTree()
ORDER BY (`c0.2`, c2)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_105356_lc SELECT if(number = 0, NULL, ('a', toLowCardinality(CAST('p', 'Nullable(String)')))), toDecimal32(number, 0) FROM numbers(2);
INSERT INTO t_105356_lc SELECT if(number = 0, NULL, ('b', toLowCardinality(CAST('q', 'Nullable(String)')))), toDecimal32(number + 5, 0) FROM numbers(2);

OPTIMIZE TABLE t_105356_lc FINAL;

SELECT count() > 0, uniqExact(_part) FROM t_105356_lc;
SELECT countIf(c0 IS NULL AND isNotNull(`c0.2`)) FROM t_105356_lc;

DROP TABLE t_105356_lc;
