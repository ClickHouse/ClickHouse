set enable_analyzer=1, group_by_use_nulls=1, optimize_injective_functions_in_group_by=1;
SELECT bitNot(bitNot(number)) + 3 FROM numbers(10) GROUP BY GROUPING SETS (('str', bitNot(bitNot(number))), ('str')) order by all;
SELECT tuple(tuple(tuple(number))) FROM numbers(10) GROUP BY GROUPING SETS (('str', tuple(tuple(number))), ('str')) order by all;
SELECT materialize(3) + 3 FROM numbers(10) GROUP BY GROUPING SETS (('str', materialize(materialize(3))), ('str')) order by all;
