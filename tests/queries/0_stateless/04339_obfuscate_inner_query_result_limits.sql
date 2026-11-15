-- Result-size limits describe the final query result, not the hidden inner query used to train
-- the `obfuscate` table function: the inner execution must not be truncated or aborted by them
-- (the inner context clears them, like subqueries and `StorageView` do).

SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(100)) LIMIT 10) SETTINGS max_result_rows = 10, result_overflow_mode = 'throw', obfuscate_seed = 'seed';
SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(100)) LIMIT 10) SETTINGS max_result_bytes = 1000, result_overflow_mode = 'throw', obfuscate_seed = 'seed';
