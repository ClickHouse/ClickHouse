-- `additional_result_filter` describes the final result of the top-level query, but the hidden query
-- behind `obfuscate` is interpreted as a standalone top-level `SELECT`, so without clearing the setting
-- for the inner context both execution paths would apply the filter to the training / generation source
-- as well. `ObfuscateSource::makeInnerContext` clears it alongside `limit` / `offset` and the result-size
-- limits.
--
-- The filter below refers to `c`, which exists only in the final result and not in the inner query, so a
-- leak into the inner context shows up as an `UNKNOWN_IDENTIFIER` rather than as a silent difference in
-- the trained model.
--
-- `obfuscate` is an effectively infinite, repeating source, so every read of it needs an explicit `LIMIT`.

SET allow_experimental_analyzer = 1;
SET obfuscate_seed = 'stable';

-- The source produced exactly the 10 rows the `LIMIT` asked for, and the filter kept the result row.
SELECT count() AS c
FROM (SELECT number FROM obfuscate(SELECT number FROM numbers(100)) LIMIT 10)
SETTINGS additional_result_filter = 'c = 10';

-- The same query with a filter that rejects the result row returns nothing - the filter really applies
-- to the final result.
SELECT count() AS c
FROM (SELECT number FROM obfuscate(SELECT number FROM numbers(100)) LIMIT 10)
SETTINGS additional_result_filter = 'c != 10';
