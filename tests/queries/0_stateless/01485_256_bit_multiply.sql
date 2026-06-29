-- Tags: no-random-settings, no-asan, no-msan, no-tsan, no-ubsan, no-debug

SET max_rows_to_read = '100M';

select count() from
(
    select toInt128(number) * number x, toInt256(number) * number y from numbers_mt(100000000) where x != y
);
