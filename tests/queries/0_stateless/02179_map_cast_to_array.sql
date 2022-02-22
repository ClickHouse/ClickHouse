WITH map(1, 'Test') AS value, 'Array(Tuple(UInt64, String))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, 'Test') AS value, 'Array(Tuple(UInt64, UInt64))' AS type
SELECT value, cast(value, type), cast(materialize(value), type); --{serverError 6}

WITH map(1, '1234') AS value, 'Array(Tuple(UInt64, UInt64))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, [1, 2, 3]) AS value, 'Array(Tuple(UInt64, Array(String)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, ['1', '2', '3']) AS value, 'Array(Tuple(UInt64, Array(UInt64)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) AS value, 'Array(Tuple(UInt64, Map(UInt64, String)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) AS value, 'Array(Tuple(UInt64, Map(UInt64, UInt64)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) AS value, 'Array(Tuple(UInt64, Array(Tuple(UInt64, String))))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) as value, 'Array(Tuple(UInt64, Array(Tuple(UInt64, UInt64))))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);
