CREATE TABLE sum_overflow(key UInt128, val UInt16) ENGINE = MergeTree ORDER BY tuple();

insert into sum_overflow SELECT number, rand() % 10000 = 0 from numbers(100000)
SETTINGS min_insert_block_size_rows = 1000,
         max_block_size =1000,
         max_threads = 2;

SELECT key, sum(val) as c
FROM sum_overflow
GROUP BY key
order by c desc
limit 100
FORMAT Null
SETTINGS group_by_overflow_mode = 'any',
         max_rows_to_group_by = 100,
         group_by_two_level_threshold_bytes = 1,
         group_by_two_level_threshold = 1,
         max_threads = 2;
