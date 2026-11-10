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

-- Carrier 3: inner element `LowCardinality(String)` that is NOT nullable. It reaches the subcolumn creator
-- as a ColumnLowCardinality with a non-nullable dictionary, so `canExtractedSubcolumnsBeInsideNullable` and
-- `canContainNull` are both false and the outer null map was dropped, tripping `Sort order of blocks
-- violated` during OPTIMIZE FINAL. The creator now promotes such an element to
-- `LowCardinality(Nullable(String))` before folding in the outer mask, so the merge no longer aborts and the
-- extracted subcolumn type can represent the outer NULLs.
DROP TABLE IF EXISTS t_105356_lc_nn;

CREATE TABLE t_105356_lc_nn
(
    c0 Nullable(Tuple(String, LowCardinality(String))),
    c2 Decimal
)
ENGINE = SummingMergeTree()
ORDER BY (`c0.2`, c2)
SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO t_105356_lc_nn SELECT if(number % 2 = 0, NULL, ('a', toLowCardinality('p'))), toDecimal32(number, 0) FROM numbers(4);
INSERT INTO t_105356_lc_nn SELECT if(number % 2 = 0, NULL, ('b', toLowCardinality('q'))), toDecimal32(number + 10, 0) FROM numbers(4);

OPTIMIZE TABLE t_105356_lc_nn FINAL;

SELECT count() > 0, uniqExact(_part) FROM t_105356_lc_nn;
SELECT toTypeName(`c0.2`) FROM t_105356_lc_nn LIMIT 1;

DROP TABLE t_105356_lc_nn;

-- Read path: extracting the same non-nullable LowCardinality element directly from disk must produce a
-- consistent `LowCardinality(Nullable(String))` column. The type creator promotes the element, but the read
-- (`create(SerializationPtr)`) path was not promoted, so the column carried a non-nullable dictionary:
-- `isNull(`c0.2`)` aborted with `ColumnUnique can't contain null values` and the value leaked the inner
-- entry for outer-NULL rows. The read path now promotes the column too. Exercise both wide and compact parts.
DROP TABLE IF EXISTS t_105356_lc_nn_read;

CREATE TABLE t_105356_lc_nn_read (c0 Nullable(Tuple(String, LowCardinality(String))))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_105356_lc_nn_read SELECT if(number % 2 = 0, NULL, ('a', toLowCardinality('p'))) FROM numbers(4);
SELECT toTypeName(`c0.2`), `c0.2`, isNull(`c0.2`), isNull(c0) FROM t_105356_lc_nn_read ORDER BY isNull(c0), `c0.2`;
DROP TABLE t_105356_lc_nn_read;

DROP TABLE IF EXISTS t_105356_lc_nn_read_compact;

CREATE TABLE t_105356_lc_nn_read_compact (c0 Nullable(Tuple(String, LowCardinality(String))))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = '100G';
INSERT INTO t_105356_lc_nn_read_compact SELECT if(number % 2 = 0, NULL, ('a', toLowCardinality('p'))) FROM numbers(4);
SELECT toTypeName(`c0.2`), `c0.2`, isNull(`c0.2`), isNull(c0) FROM t_105356_lc_nn_read_compact ORDER BY isNull(c0), `c0.2`;
DROP TABLE t_105356_lc_nn_read_compact;
