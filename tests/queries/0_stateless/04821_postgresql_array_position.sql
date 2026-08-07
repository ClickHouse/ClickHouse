-- PostgreSQL compatibility function `array_position`: the 1-based position of the first occurrence,
-- or NULL when the element does not occur (where the native `indexOf` returns 0). It backs the
-- `search_path`-ordered schema discovery query of the `postgresql` table function and engine.
SELECT array_position(['a', 'b', 'c'], 'b');
SELECT array_position(['a', 'b', 'c'], 'z');
SELECT array_position([1, 2, 3, 2], 2);
SELECT array_position([], 1);
SELECT toTypeName(array_position([1, 2, 3], 2));
SELECT array_position(current_schemas(false), currentDatabase()) FROM system.one;
SELECT array_position(['x', 'y'], materialize('y'));
SELECT number, array_position(['b', 'a'], if(number > 0, 'a', 'b')) FROM numbers(2);
