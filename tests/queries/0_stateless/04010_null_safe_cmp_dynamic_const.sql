-- Regression test: null-safe comparison with a const Dynamic/Variant column should not throw
-- "Bad cast from type ColumnConst to ColumnDynamic"
SELECT CAST(toUInt8(0), 'Dynamic(max_types=2)') <=> materialize(NULL);
SELECT materialize(NULL) <=> CAST(toUInt8(0), 'Dynamic(max_types=2)');
SELECT CAST(toUInt8(0), 'Dynamic(max_types=2)') IS NOT DISTINCT FROM materialize(NULL);
SELECT CAST(NULL, 'Dynamic(max_types=2)') <=> materialize(NULL);
SELECT CAST(toUInt8(0), 'Dynamic(max_types=2)') IS DISTINCT FROM materialize(NULL);
SELECT CAST(NULL, 'Dynamic(max_types=2)') IS DISTINCT FROM materialize(NULL);
