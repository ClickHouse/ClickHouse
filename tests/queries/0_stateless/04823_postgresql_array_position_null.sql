-- The PostgreSQL compatibility function `array_position` follows PostgreSQL's NULL semantics:
-- a NULL element is a genuine search key that finds the first NULL of the array, instead of the
-- common ClickHouse default-null shortcut that would collapse the whole result to NULL.
SELECT array_position([NULL, 1], NULL);
SELECT array_position([1, NULL, NULL], NULL);
SELECT array_position([1, 2], NULL);
SELECT array_position([NULL, 'a', 'b'], 'b');
SELECT array_position([NULL, 'a', 'b'], 'z');
SELECT array_position(CAST([] AS Array(Nullable(Int32))), NULL);
-- A NULL element arriving from a column, not only from a literal.
SELECT number, array_position([NULL, 1, 2], if(number = 0, CAST(NULL AS Nullable(UInt8)), CAST(number AS Nullable(UInt8)))) FROM numbers(3);
-- A literal NULL in place of the array yields NULL, as in PostgreSQL.
SELECT array_position(NULL, 1);
SELECT toTypeName(array_position([NULL, 1], NULL));
