-- If the untuple() function has an alias, and if the tuple element has an explicit name, we want to use it to
-- generate the resulting column name.
-- Check all combinations of tuple element alias and untuple() alias. Also to avoid that we generate the same
-- result column names and confuse query analysis (see #26179), test also two untuple() calls in one SElECT
-- with the same types and aliases.

SELECT '-- tuple element alias';

SELECT untuple(tuple(1)::Tuple(a Int)), untuple(tuple('s')::Tuple(a String)) FORMAT Vertical SETTINGS enable_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(a Int)), untuple(tuple('s')::Tuple(a String)) FORMAT Vertical SETTINGS enable_analyzer = 1;

SELECT untuple(tuple(1)::Tuple(a Int)), untuple(tuple(1)::Tuple(a Int)) FORMAT Vertical SETTINGS enable_analyzer = 0; -- { serverError DUPLICATE_COLUMN }
SELECT untuple(tuple(1)::Tuple(a Int)), untuple(tuple(1)::Tuple(a Int)) FORMAT Vertical SETTINGS enable_analyzer = 1; -- Bug: doesn't throw an exception

SELECT '-- tuple element alias + untuple() alias';

SELECT untuple(tuple(1)::Tuple(a Int)) x, untuple(tuple('s')::Tuple(a String)) y FORMAT Vertical SETTINGS enable_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(a Int)) x, untuple(tuple('s')::Tuple(a String)) y FORMAT Vertical SETTINGS enable_analyzer = 1;

SELECT untuple(tuple(1)::Tuple(a Int)) x, untuple(tuple(1)::Tuple(a Int)) x FORMAT Vertical SETTINGS enable_analyzer = 0; -- { serverError DUPLICATE_COLUMN }
SELECT untuple(tuple(1)::Tuple(a Int)) x, untuple(tuple(1)::Tuple(a Int)) x FORMAT Vertical SETTINGS enable_analyzer = 1; -- Bug: doesn't throw an exception

SELECT '-- untuple() alias';

SELECT untuple(tuple(1)::Tuple(Int)) x, untuple(tuple('s')::Tuple(String)) y FORMAT Vertical SETTINGS enable_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(Int)) x, untuple(tuple('s')::Tuple(String)) y FORMAT Vertical SETTINGS enable_analyzer = 1;

SELECT untuple(tuple(1)::Tuple(Int)) x, untuple(tuple(1)::Tuple(Int)) x FORMAT Vertical SETTINGS enable_analyzer = 0; -- { serverError DUPLICATE_COLUMN }
SELECT untuple(tuple(1)::Tuple(Int)) x, untuple(tuple(1)::Tuple(Int)) x FORMAT Vertical SETTINGS enable_analyzer = 1; -- Bug: doesn't throw an exception

SELECT '-- no aliases';

SELECT untuple(tuple(1)::Tuple(Int)), untuple(tuple('s')::Tuple(String)) FORMAT Vertical SETTINGS enable_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(Int)), untuple(tuple('s')::Tuple(String)) FORMAT Vertical SETTINGS enable_analyzer = 1;

SELECT untuple(tuple(1)::Tuple(Int)), untuple(tuple(1)::Tuple(Int)) FORMAT Vertical SETTINGS enable_analyzer = 0; -- { serverError DUPLICATE_COLUMN }
SELECT untuple(tuple(1)::Tuple(Int)), untuple(tuple(1)::Tuple(Int)) FORMAT Vertical SETTINGS enable_analyzer = 1; -- Bug: doesn't throw an exception

SELECT '-- tuple() loses the column names (would be good to fix, see #36773)';
SELECT untuple(tuple(1 as a)) as t FORMAT Vertical SETTINGS enable_analyzer = 0, enable_named_columns_in_function_tuple = 0;
SELECT untuple(tuple(1 as a)) as t FORMAT Vertical SETTINGS enable_analyzer = 1, enable_named_columns_in_function_tuple = 0;

SELECT '-- tuple() with enable_named_columns_in_function_tuple = 1 and enable_analyzer = 1 keeps the column names';
SELECT untuple(tuple(1 as a)) as t FORMAT Vertical SETTINGS enable_analyzer = 1, enable_named_columns_in_function_tuple = 1;

SELECT '-- thankfully JSONExtract() keeps them';
SELECT untuple(JSONExtract('{"key": "value"}', 'Tuple(key String)')) x FORMAT Vertical SETTINGS enable_analyzer = 0;
SELECT untuple(JSONExtract('{"key": "value"}', 'Tuple(key String)')) x FORMAT Vertical SETTINGS enable_analyzer = 1;
