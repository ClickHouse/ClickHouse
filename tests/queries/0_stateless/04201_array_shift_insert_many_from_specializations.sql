-- Test: exercises specialized `insertManyFrom` for `ColumnFixedString`, `ColumnDecimal`,
-- `ColumnNullable`, `ColumnTuple`, and `ColumnMap` via `arrayShiftRight`/`arrayShiftLeft`.
-- These specializations replace the default loop-of-`insertFrom` path. No existing test
-- exercises `arrayShift*` with these element types.
--
-- Covers:
--   src/Columns/ColumnFixedString.cpp:105 — `ColumnFixedString::insertManyFrom`
--   src/Columns/ColumnDecimal.h:63        — `ColumnDecimal::insertManyFrom`
--   src/Columns/ColumnNullable.cpp:298    — `ColumnNullable::insertManyFrom`
--   src/Columns/ColumnTuple.cpp:259       — `ColumnTuple::insertManyFrom`
--   src/Columns/ColumnMap.cpp:205         — `ColumnMap::insertManyFrom`
--
-- Risk if these paths break: wrong/garbage values in `arrayShift*` results, JOIN result
-- corruption (HashJoin uses the same `insertManyFrom` to replicate left-side rows).

SELECT '== FixedString ==';
SELECT arrayShiftRight([toFixedString('aa', 2), toFixedString('bb', 2), toFixedString('cc', 2)], 2, toFixedString('xx', 2));
SELECT arrayShiftLeft([toFixedString('aa', 2), toFixedString('bb', 2), toFixedString('cc', 2)], 2, toFixedString('xx', 2));

SELECT '== Decimal128 ==';
SELECT arrayShiftRight([toDecimal128(1.5, 3), toDecimal128(2.5, 3), toDecimal128(3.5, 3)], 2, toDecimal128(99.9, 3));
SELECT arrayShiftLeft([toDecimal128(1.5, 3), toDecimal128(2.5, 3), toDecimal128(3.5, 3)], 2, toDecimal128(99.9, 3));

SELECT '== Decimal64 ==';
SELECT arrayShiftRight([toDecimal64(1.5, 3), toDecimal64(2.5, 3), toDecimal64(3.5, 3)], 2, toDecimal64(99.9, 3));

SELECT '== Nullable(Int64) (insertManyFrom on a NULL position) ==';
SELECT arrayShiftRight(materialize([1::Nullable(Int64), NULL, 3]), 2, 99);
SELECT arrayShiftLeft(materialize([1::Nullable(Int64), 2, NULL]), 2, 99);

SELECT '== Tuple ==';
SELECT arrayShiftRight([(1, 'a'), (2, 'b'), (3, 'c')], 2, (0, 'z'));
SELECT arrayShiftLeft([(1, 'a'), (2, 'b'), (3, 'c')], 2, (0, 'z'));

SELECT '== Map ==';
SELECT arrayShiftRight([map('a', 1), map('b', 2), map('c', 3)], 2, map('z', 0));
SELECT arrayShiftLeft([map('a', 1), map('b', 2), map('c', 3)], 2, map('z', 0));

SELECT '== JOIN exercising insertManyFrom on left FixedString and Decimal columns ==';
DROP TABLE IF EXISTS t_imf_left;
DROP TABLE IF EXISTS t_imf_right;
CREATE TABLE t_imf_left  (k UInt8, fs FixedString(3), dec Decimal128(2)) ENGINE = Memory;
CREATE TABLE t_imf_right (k UInt8) ENGINE = Memory;
INSERT INTO t_imf_left  VALUES (1, 'abc', 12.34), (2, 'def', 56.78);
INSERT INTO t_imf_right VALUES (1), (1), (1), (2), (2);
-- Each left row matches multiple right rows → HashJoin uses `insertManyFrom`
-- to replicate left-side `fs`/`dec` columns.
SELECT l.fs, l.dec FROM t_imf_left l INNER JOIN t_imf_right r ON l.k = r.k ORDER BY l.fs, l.dec;
DROP TABLE t_imf_left;
DROP TABLE t_imf_right;
