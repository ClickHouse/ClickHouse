DROP TABLE IF EXISTS subcolumns;

CREATE TABLE subcolumns
(
    t Tuple
    (
        a Array(Nullable(UInt32)),
        u UInt32,
        s Nullable(String)
    ),
    arr Array(Nullable(String)),
    arr2 Array(Array(Nullable(String))),
    lc LowCardinality(String),
    nested Nested(col1 String, col2 Nullable(UInt32))
)
ENGINE = MergeTree order by tuple() SETTINGS min_bytes_for_wide_part = '10M';

INSERT INTO subcolumns VALUES (([1, NULL], 2, 'a'), ['foo', NULL, 'bar'], [['123'], ['456', '789']], 'qqqq', ['zzz', 'xxx'], [42, 43]);
SELECT * FROM subcolumns;
SELECT t.a, t.u, t.s, nested.col1, nested.col2, lc FROM subcolumns;
SELECT t.a.size0, t.a.null, t.u, t.s, t.s.null FROM subcolumns;
SELECT sumArray(arr.null), sum(arr.size0) FROM subcolumns;
SELECT arr2, arr2.size0, arr2.size1, arr2.null FROM subcolumns;
-- SELECT nested.col1, nested.col2, nested.col1.size0, nested.col2.size0, nested.col2.null FROM subcolumns;

DROP TABLE IF EXISTS subcolumns;

CREATE TABLE subcolumns
(
    t Tuple
    (
        a Array(Nullable(UInt32)),
        u UInt32,
        s Nullable(String)
    ),
    arr Array(Nullable(String)),
    arr2 Array(Array(Nullable(String))),
    lc LowCardinality(String),
    nested Nested(col1 String, col2 Nullable(UInt32))
)
ENGINE = MergeTree order by tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO subcolumns VALUES (([1, NULL], 2, 'a'), ['foo', NULL, 'bar'], [['123'], ['456', '789']], 'qqqq', ['zzz', 'xxx'], [42, 43]);
SELECT * FROM subcolumns;
SELECT t.a, t.u, t.s, nested.col1, nested.col2, lc FROM subcolumns;
SELECT t.a.size0, t.a.null, t.u, t.s, t.s.null FROM subcolumns;
SELECT sumArray(arr.null), sum(arr.size0) FROM subcolumns;
SELECT arr2, arr2.size0, arr2.size1, arr2.null FROM subcolumns;
-- SELECT nested.col1, nested.col2, nested.size0, nested.size0, nested.col2.null FROM subcolumns;

DROP TABLE subcolumns;
