
set optimize_normalize_count_variants = 1;

explain syntax select count(), count(1), count(-1), sum(1), count(null);
