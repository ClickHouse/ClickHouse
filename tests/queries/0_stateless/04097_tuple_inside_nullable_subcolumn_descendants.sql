-- { echo }

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nullable_tuple_desc_wide;
CREATE TABLE t_nullable_tuple_desc_wide
(
    key UInt64,
    tup Nullable(Tuple(`a.b` Nullable(String), s Nullable(String), u UInt64))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_tuple_desc_wide VALUES
    (1, ('hello', 'x', 10)),
    (2, (NULL, 'y', 20)),
    (3, NULL),
    (4, ('z', NULL, 40));

SELECT key, tup.`a.b`, coalesce(tup.`a.b`.null, toUInt8(1)), ifNull(tup.`a.b`.size, toUInt64(999)), isNull(tup.`a.b`) FROM t_nullable_tuple_desc_wide ORDER BY key;
SELECT key, tup.s, coalesce(tup.s.null, toUInt8(1)), ifNull(tup.s.size, toUInt64(999)), isNull(tup.s) FROM t_nullable_tuple_desc_wide ORDER BY key;
SELECT key, tup.`a.b`, coalesce(tup.`a.b`.null, toUInt8(1)), tup.s, coalesce(tup.s.null, toUInt8(1)) FROM t_nullable_tuple_desc_wide ORDER BY key;
DROP TABLE t_nullable_tuple_desc_wide;

DROP TABLE IF EXISTS t_nullable_tuple_desc_compact;
CREATE TABLE t_nullable_tuple_desc_compact
(
    key UInt64,
    tup Nullable(Tuple(`a.b` Nullable(String), s Nullable(String), u UInt64))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 1000000000;

INSERT INTO t_nullable_tuple_desc_compact VALUES
    (1, ('hello', 'x', 10)),
    (2, (NULL, 'y', 20)),
    (3, NULL),
    (4, ('z', NULL, 40));

SELECT key, tup.`a.b`, coalesce(tup.`a.b`.null, toUInt8(1)), ifNull(tup.`a.b`.size, toUInt64(999)), tup.s, coalesce(tup.s.null, toUInt8(1)), ifNull(tup.s.size, toUInt64(999)) FROM t_nullable_tuple_desc_compact ORDER BY key;
DROP TABLE t_nullable_tuple_desc_compact;

DROP TABLE IF EXISTS t_nullable_tuple_desc_escaped;
CREATE TABLE t_nullable_tuple_desc_escaped
(
    key UInt64,
    tup Nullable(Tuple(`a\`b` Nullable(String)))
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_tuple_desc_escaped VALUES
    (1, ('hello')),
    (2, (NULL)),
    (3, NULL),
    (4, ('z'));

SELECT key, tup.`a\`b`, coalesce(tup.`a\`b`.null, toUInt8(1)), ifNull(tup.`a\`b`.size, toUInt64(999)), isNull(tup.`a\`b`) FROM t_nullable_tuple_desc_escaped ORDER BY key;
DROP TABLE t_nullable_tuple_desc_escaped;
