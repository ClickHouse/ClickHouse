-- Tags: no-fasttest
-- no-fasttest: needs Parquet

SET allow_experimental_nullable_tuple_type = 1;
SET engine_file_truncate_on_insert = 1;
SET print_pretty_type_names = 0;

-- Infer `OPTIONAL` Parquet group as `Nullable(Tuple(...))`

INSERT INTO FUNCTION file(currentDatabase() || '_04653.parquet', Parquet, 'p Nullable(Tuple(a UInt8, b String))')
    SELECT * FROM values('p Nullable(Tuple(a UInt8, b String))', ((1, 'x')), (NULL), ((3, 'z')));

DESCRIBE file(currentDatabase() || '_04653.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653.parquet', Parquet);

INSERT INTO FUNCTION file(
    currentDatabase() || '_04653_nullable_element.parquet',
    Parquet,
    'p Nullable(Tuple(a Nullable(UInt8), b String))')
SELECT *
FROM values(
    'p Nullable(Tuple(a Nullable(UInt8), b String))',
    ((1, 'x')),
    (NULL),
    ((NULL, 'z')));

DESCRIBE file(currentDatabase() || '_04653_nullable_element.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653_nullable_element.parquet', Parquet);

INSERT INTO FUNCTION file(currentDatabase() || '_04653_point.parquet', Parquet, 'p Nullable(Point)')
    SELECT * FROM values('p Nullable(Point)', ((1, 2)), (NULL));

DESCRIBE file(currentDatabase() || '_04653_point.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653_point.parquet', Parquet);

INSERT INTO FUNCTION file(currentDatabase() || '_04653_array.parquet', Parquet, 'p Array(Nullable(Point))')
    SELECT * FROM values('p Array(Nullable(Point))', ([(1, 2), NULL]), ([]));

DESCRIBE file(currentDatabase() || '_04653_array.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653_array.parquet', Parquet);

INSERT INTO FUNCTION file(currentDatabase() || '_04653_nested.parquet', Parquet, 'n Nullable(Tuple(x Int64, t Tuple(y Int64, z String)))')
    SELECT if(number = 1, NULL, (number, (number * 10, 'y'))) FROM numbers(3);

DESCRIBE file(currentDatabase() || '_04653_nested.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653_nested.parquet', Parquet);

INSERT INTO FUNCTION file(currentDatabase() || '_04653_map.parquet', Parquet, 'm Map(String, Nullable(Tuple(x UInt32)))')
    SELECT map('a', if(number = 1, NULL, tuple(toUInt32(number)))) FROM numbers(2);

DESCRIBE file(currentDatabase() || '_04653_map.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653_map.parquet', Parquet);

-- Nullable element without optional parent group stays inside `Tuple`

INSERT INTO FUNCTION file(currentDatabase() || '_04653_inner.parquet', Parquet, 'p Tuple(a Nullable(UInt8))')
    SELECT * FROM values('p Tuple(a Nullable(UInt8))', (tuple(1)), (tuple(NULL)));

DESCRIBE file(currentDatabase() || '_04653_inner.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653_inner.parquet', Parquet);

-- Without `allow_experimental_nullable_tuple_type`, push group null map onto leaves

SET allow_experimental_nullable_tuple_type = 0;

DESCRIBE file(currentDatabase() || '_04653.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653.parquet', Parquet);

DESCRIBE file(currentDatabase() || '_04653_nullable_element.parquet', Parquet);
SELECT * FROM file(currentDatabase() || '_04653_nullable_element.parquet', Parquet);
