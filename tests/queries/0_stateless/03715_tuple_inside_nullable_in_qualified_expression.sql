-- This form does not work even without Nullable(Tuple) for old analzyer
SET enable_analyzer = 1;

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS qualified_match_nullable_tuple_direct;

CREATE TABLE qualified_match_nullable_tuple_direct
(
    id UInt8,
    t  Nullable(Tuple(a Int32, s String))
)
ENGINE = TinyLog;

INSERT INTO qualified_match_nullable_tuple_direct VALUES
    (1, (1, 'a')),
    (2, NULL),
    (3, (3, 'c'));

SELECT 'qualified_match_nullable_tuple_direct';
SELECT
    id,
    t.*,
    toTypeName(t)
FROM qualified_match_nullable_tuple_direct
ORDER BY id;

DROP TABLE IF EXISTS qualified_match_join_left;
DROP TABLE IF EXISTS qualified_match_join_right;

CREATE TABLE qualified_match_join_left
(
    x Int32,
    t Nullable(Tuple(a Int32, s String))
)
ENGINE = MergeTree
ORDER BY x;

CREATE TABLE qualified_match_join_right
(
    x Int32,
    t Tuple(a Int32, s String)
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO qualified_match_join_left VALUES
    (1, (1, 'a')),
    (2, (2, 'b'));

INSERT INTO qualified_match_join_right VALUES
    (2, (2, 'b')),
    (3, (3, 'c'));

SET join_use_nulls = 1;

SELECT 'qualified_match_full_join_nullable_tuple_key';
SELECT
    coalesce(qualified_match_join_left.x, qualified_match_join_right.x) AS x,
    t.*,
    toTypeName(t)
FROM qualified_match_join_left
FULL JOIN qualified_match_join_right USING (t)
ORDER BY x;

SET join_use_nulls = 0;

DROP TABLE IF EXISTS qualified_match_join_left;
DROP TABLE IF EXISTS qualified_match_join_right;

SELECT 'qualified_match_array_of_nullable_tuple';
SELECT
    arr.*,
    toTypeName(arr)
FROM
(
    SELECT
        [tuple(1, 'a'), NULL, tuple(3, 'c')]::Array(Nullable(Tuple(a Int32, s String))) AS arr
) AS src;


SELECT 'qualified_match_map_of_nullable_tuple';
SELECT
    m.*,
    toTypeName(m)
FROM
(
    SELECT
        map(
            'k1', tuple(1, 'a'),
            'k2', NULL,
            'k3', tuple(3, 'c')
        )::Map(String, Nullable(Tuple(a Int32, s String))) AS m
) AS src;
