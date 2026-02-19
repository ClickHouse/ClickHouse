DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, c UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab SELECT number, 0 FROM numbers(1000000);

SYSTEM STOP MERGES tab;

ALTER TABLE tab UPDATE c = 1 WHERE id < 500000 SETTINGS mutations_sync = 0;

-- Mutation is not applied on the fly, expect 0 results, this also gets cached in the query condition cache
SELECT count() FROM tab WHERE c = 1 SETTINGS use_query_condition_cache = 1, apply_mutations_on_fly = 0;

-- Same query as before but with different settings. Expect 500k results. Returning 0 results (because it was cached in the query
-- condition cache) would be an error.
SELECT count() FROM tab WHERE c = 1 SETTINGS use_query_condition_cache = 1, apply_mutations_on_fly = 1;

-- Again same query as before, but without cache lookup. Expect 500k results.
SELECT count() FROM tab WHERE c = 1 SETTINGS use_query_condition_cache = 0, apply_mutations_on_fly = 1;

DROP TABLE tab;
