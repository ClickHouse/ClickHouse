-- { echoOn }

-- tuple() loses the column names (would be good to fix, see #36773)
select untuple(tuple(1 as a, 2 as b)) format TSVWithNames;

-- explicitly specified tuple element names are used as column names
select untuple(tuple(1, 2)::Tuple(a Int, b Int)) format TSVWithNames;

-- indexes are used when untuple() has an alias (backward compatibility)
select untuple(tuple(1, 2)::Tuple(a Int, b Int)) u format TSVWithNames;

-- default names are used when there are no tuple element names and no untuple alias
select untuple(tuple(1, 2)::Tuple(Int, Int)) format TSVWithNames;
