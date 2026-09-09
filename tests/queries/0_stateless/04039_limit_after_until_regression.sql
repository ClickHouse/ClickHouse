-- Global `LIMIT` should be shared across streams.
SELECT count() FROM (SELECT number FROM numbers_mt(2000) LIMIT 1 AFTER number >= 0) SETTINGS max_threads = 8, max_block_size = 1;

-- `LIMIT 0` should return no rows.
SELECT count() FROM (SELECT number FROM numbers(10) LIMIT 0 AFTER number >= 0);

-- If `UNTIL` appears before `AFTER`, the result is empty.
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number >= 6 UNTIL number >= 2);

-- `UNTIL` can be used without numeric `LIMIT`.
SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number >= 3;

-- `AFTER` can be used without numeric `LIMIT`.
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 3;
