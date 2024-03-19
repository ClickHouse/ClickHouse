set allow_experimental_analyzer = 1;
set group_by_use_nulls = 1;
set optimize_group_by_function_keys = 1;
set optimize_injective_functions_in_group_by = 1;

SELECT 3 + 3 from numbers(10) GROUP BY GROUPING SETS (('str'), (3 + 3));
SELECT materialize(3) from numbers(10) GROUP BY GROUPING SETS (('str'), (materialize(3)));
SELECT ignore(3) from numbers(10) GROUP BY GROUPING SETS (('str'), (ignore(3)));
SELECT materialize(ignore(3)) from numbers(10) GROUP BY GROUPING SETS (('str'), (materialize(ignore(3))));
SELECT ignore(materialize(3)) from numbers(10) GROUP BY GROUPING SETS (('str'), (ignore(materialize(3))));

