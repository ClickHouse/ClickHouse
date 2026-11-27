-- A declared `dynamic` column type is rejected by name. `dynamic([...])` literals lower to
-- ClickHouse `Array`, but a schema annotation carries no element type, so mapping the type
-- name (the old implementation used `String`) breaks every downstream construct that expects
-- an array (`a[0]`, `mv-expand a`, `array_length(a)`).

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

datatable (a: dynamic) [dynamic([1, 2])] | count; -- { clientError SYNTAX_ERROR }
datatable (a: int, b: dynamic) [1, dynamic([1])] | count; -- { clientError SYNTAX_ERROR }
print extract('x=([0-9.]+)', 1, 'hello x=45.6|wo', typeof(dynamic)); -- { clientError SYNTAX_ERROR }
let f = (x: dynamic) { array_length(x) }; print f(dynamic([1, 2])); -- { clientError SYNTAX_ERROR }

-- Dynamic array literals themselves still work.
print '-- dynamic array literals are unaffected --';
print a = dynamic([1, 2, 3]) | project n = array_length(a), first = a[0];
