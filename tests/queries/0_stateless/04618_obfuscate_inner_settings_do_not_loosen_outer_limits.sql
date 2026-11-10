-- The read limits of the outer query must reach the hidden inner query of `obfuscate`,
-- even when the inner query tries to loosen them with its own SETTINGS clause.
-- https://github.com/ClickHouse/ClickHouse/pull/42701

-- Baseline: the outer limit alone bounds the inner training read.
SELECT * FROM obfuscate(SELECT number FROM numbers(100000)) LIMIT 1
SETTINGS max_rows_to_read = 1000, obfuscate_seed = 'seed'; -- { serverError TOO_MANY_ROWS }

-- An inner SETTINGS override must not loosen the outer limit.
SELECT * FROM obfuscate(SELECT number FROM numbers(100000) SETTINGS max_rows_to_read = 1000000) LIMIT 1
SETTINGS max_rows_to_read = 1000, obfuscate_seed = 'seed'; -- { serverError TOO_MANY_ROWS }

-- A permissive outer limit lets the query run.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(100)) LIMIT 10)
SETTINGS max_rows_to_read = 1000000, obfuscate_seed = 'seed';
