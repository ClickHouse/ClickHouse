# Tags: long

set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set allow_experimental_dynamic_type = 1;
set optimize_functions_to_subcolumns = 0;

{% for engine in ['Memory', 'MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000', 'MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1'] -%}

SELECT '--- {{ engine }} ---';



{% endfor -%}
