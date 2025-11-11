SET transform_null_in = 1;
SELECT CAST(NULL, 'Nullable(Int32)') IN [NULL, 1];

SET transform_null_in = 0;
SELECT CAST(NULL, 'Nullable(Int32)') IN [NULL, 1];

SET transform_null_in = 1;
SELECT CAST(NULL, 'Nullable(Int32)') IN (NULL, 1);

SET transform_null_in = 0;
SELECT CAST(NULL, 'Nullable(Int32)') IN (NULL, 1);

SELECT CAST('a', 'String') IN [NULL, 'a'];

SELECT 2 IN (1, 2, 3);

SELECT CAST(tuple(), 'Tuple()') IN (tuple());

SELECT CAST((200, 3), 'Tuple(Int32, Int64)') IN [(200, 3), (200, 4)];

SELECT CAST((200, 3), 'Tuple(Int32, Int64)') IN ((200, 3), (200, 4));

SELECT CAST(tuple(tuple(10)), 'Tuple(Tuple(Int32))') IN (tuple(tuple(10)));

SELECT CAST('x', 'LowCardinality(String)') IN ['x', 'y'];

SELECT CAST(('x', 1), 'Tuple(LowCardinality(String), Int32)') IN [('x', 1), ('z', 2)];

SELECT CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)') IN ['a', 'c'];

SELECT CAST(('a', 1), 'Tuple(Enum8(\'a\' = 1, \'b\' = 2), Int32)') IN [('a', 1), ('c', 1)];

SELECT CAST((1, 2), 'Tuple(Int32, Int32)') IN [(1, 2, 3)];-- { serverError INCORRECT_ELEMENT_OF_SET }

SELECT CAST((1, 2), 'Tuple(Int32, Int32)') IN (1, 2);

SET transform_null_in = 1;
SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)') IN [(NULL, 3), (1, 3)];

SET transform_null_in = 0;
SELECT CAST((NULL, 3), 'Tuple(Nullable(Int32), Int32)') IN [(NULL, 3), (1, 3)];

SELECT 1 IN [[1, 2]];-- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
