-- { echo }
select 1+number+multiIf(number == 1, cityHash64(number), number) from numbers(1) settings compile_expressions=1, min_count_to_compile_expression=0;
