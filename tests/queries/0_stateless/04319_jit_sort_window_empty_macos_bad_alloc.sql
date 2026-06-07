SET compile_sort_description = 1;
SET min_count_to_compile_sort_description = 0;

SELECT row_number() OVER (PARTITION BY number ORDER BY number) FROM numbers(0) FORMAT Null;
