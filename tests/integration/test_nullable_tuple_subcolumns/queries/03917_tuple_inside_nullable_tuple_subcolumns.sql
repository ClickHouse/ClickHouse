-- Tuple-related queries from tests/queries/0_stateless/03917_tuple_inside_nullable_tuple_subcolumns.sql.

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS x;
CREATE TABLE x
(
    t Nullable(Tuple(a Tuple(x UInt32, y String), b String))
) ENGINE = Memory;

INSERT INTO x VALUES (((1, 'aa'), 'B')), (NULL);

SELECT
    toTypeName(t.a), t.a,
    toTypeName(t.a.x), t.a.x,
    toTypeName(t.a.y), t.a.y
FROM x;

SELECT
    toTypeName(tupleElement(t, 'a')),
    tupleElement(t, 'a'),
    toTypeName(tupleElement(tupleElement(t, 'a'), 'x')),
    tupleElement(tupleElement(t, 'a'), 'x'),
    toTypeName(tupleElement(tupleElement(t, 'a'), 'y')),
    tupleElement(tupleElement(t, 'a'), 'y')
FROM x;
