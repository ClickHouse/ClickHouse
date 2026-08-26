
set optimize_normalize_count_variants = 1;

explain syntax select count(), count(1), count(-1), sum(1), count(null);

set aggregate_functions_null_for_empty = 1;

explain syntax select sum(1) from numbers(10) where 0;
