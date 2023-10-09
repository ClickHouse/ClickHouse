-- If the untuple() function has an alias, and the tuple element has an explicit name,
-- we want to use it to generate the resulting column name. Check all permutations of
-- aliases. Also use different tuple types to check that we don't generate different
-- columns with the same name (see #26179).

SELECT '-- tuple element alias';
SELECT untuple(tuple(1)::Tuple(a Int)), untuple(tuple('s')::Tuple(a String)) FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(a Int)), untuple(tuple('s')::Tuple(a String)) FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 1;

SELECT '-- tuple element alias, untuple() alias';
SELECT untuple(tuple(1)::Tuple(a Int)) x, untuple(tuple('s')::Tuple(a String)) y FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(a Int)) x, untuple(tuple('s')::Tuple(a String)) y FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 1;

SELECT '-- untuple() alias';
SELECT untuple(tuple(1)::Tuple(Int)) x, untuple(tuple('s')::Tuple(String)) y FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(Int)) x, untuple(tuple('s')::Tuple(String)) y FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 1;

SELECT '-- no aliases';
SELECT untuple(tuple(1)::Tuple(Int)), untuple(tuple('s')::Tuple(String)) FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 0;
SELECT untuple(tuple(1)::Tuple(Int)), untuple(tuple('s')::Tuple(String)) FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 1;

SELECT '-- tuple() loses the column names (would be good to fix, see #36773)';
SELECT untuple(tuple(1 as a)) as t FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 0;
SELECT untuple(tuple(1 as a)) as t FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 1;

SELECT '-- thankfully JSONExtract() keeps them';
SELECT untuple(JSONExtract('{"key": "value"}', 'Tuple(key String)')) x FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 0;
SELECT untuple(JSONExtract('{"key": "value"}', 'Tuple(key String)')) x FORMAT TSVWithNames SETTINGS allow_experimental_analyzer = 1;
