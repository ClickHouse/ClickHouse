-- "Tests" current_timestamp() which is an alias of now().
-- Since the function is non-deterministic, only check that no bad things happen (don't check the returned value).

SELECT count() FROM (SELECT current_timestamp());
SELECT count() FROM (SELECT CURRENT_TIMESTAMP());
SELECT count() FROM (SELECT current_TIMESTAMP());
