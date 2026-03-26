DROP TABLE IF EXISTS t_s2_bad0;
CREATE TABLE t_s2_bad0
(
    id UInt64,
    p Polygon,
    PROJECTION pr INDEX p TYPE s2(max_cells = 0)
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_s2_bad1;
CREATE TABLE t_s2_bad1
(
    id UInt64,
    p Polygon,
    PROJECTION pr INDEX p TYPE s2(min_level = 20, max_level = 10)
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_s2_bad2;
CREATE TABLE t_s2_bad2
(
    id UInt64,
    p Polygon,
    PROJECTION pr INDEX p TYPE s2(max_level = 31)
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_s2_bad3;
CREATE TABLE t_s2_bad3
(
    id UInt64,
    p Polygon,
    PROJECTION pr INDEX p TYPE s2(foo = 1)
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS t_s2_bad4;
CREATE TABLE t_s2_bad4
(
    id UInt64,
    _part_offset UInt64,
    p Polygon,
    PROJECTION pr INDEX p TYPE s2()
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

DROP TABLE IF EXISTS t_s2_bad5;
CREATE TABLE t_s2_bad5
(
    id UInt64,
    s String,
    PROJECTION pr INDEX s TYPE s2(strict_decode = 1)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_s2_bad5 VALUES (1, 'abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS t_s2_bad5;
