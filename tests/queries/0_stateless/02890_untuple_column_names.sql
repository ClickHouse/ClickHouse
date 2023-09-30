-- { echoOn }

-- If the untuple() function has an alias, and the tuple element has an explicit name,
-- we want to use it to generate the resulting column name. Check all permutations of
-- aliases. Also use different tuple types to check that we don't generate different
-- columns with the same name (see #26179).

select untuple(tuple(1)::Tuple(a Int)), untuple(tuple('s')::Tuple(a String)) format TSVWithNames;

select untuple(tuple(1)::Tuple(a Int)) x, untuple(tuple('s')::Tuple(a String)) y format TSVWithNames;

select untuple(tuple(1)::Tuple(Int)) x, untuple(tuple('s')::Tuple(String)) y format TSVWithNames;

select untuple(tuple(1)::Tuple(Int)), untuple(tuple('s')::Tuple(String)) format TSVWithNames;

-- tuple() loses the column names (would be good to fix, see #36773)
select untuple(tuple(1 as a)) as t format TSVWithNames;

-- thankfully JSONExtract() keeps them
select untuple(JSONExtract('{"key": "value"}', 'Tuple(key String)')) x format TSVWithNames;
