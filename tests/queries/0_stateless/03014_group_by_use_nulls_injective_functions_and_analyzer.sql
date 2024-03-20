set allow_experimental_analyzer=1, group_by_use_nulls=1, optimize_injective_functions_in_group_by=1;
SELECT intExp2(intExp2(number)) + 3 FROM numbers(10) GROUP BY GROUPING SETS (('str', intExp2(intExp2(number))), ('str')) order by all;
SELECT tuple(tuple(tuple(number))) FROM numbers(10) GROUP BY GROUPING SETS (('str', tuple(tuple(number))), ('str')) order by all;
SELECT materialize(3) + 3 FROM numbers(10) GROUP BY GROUPING SETS (('str', materialize(materialize(3))), ('str')) order by all;

