set enable_analyzer=1;

explain description=1, optimize=0 select sum(number + 1) from numbers(10) settings query_plan_max_step_description_length=3;
explain description=1, optimize=1 select sum(number + 1) from numbers(10) settings query_plan_max_step_description_length=3;
