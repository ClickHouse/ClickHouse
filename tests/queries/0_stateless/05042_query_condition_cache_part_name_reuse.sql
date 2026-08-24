-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Block numbers restart for a re-created table, so a `DROP` + `CREATE` that reuses the table UUID
-- produces a new part carrying a name an earlier part already used. The query condition cache must
-- not serve the earlier part's verdict for the new one, or rows silently go missing.

SET enable_analyzer = 1; -- the cache is only consulted with the analyzer
SET use_query_condition_cache = 1;
SET use_statistics_for_part_pruning = 0; -- randomized auto_statistics_types could prune the part first
SET database_replicated_allow_explicit_uuid = 1; -- pinning the UUID is the point of the test

SYSTEM DROP QUERY CONDITION CACHE;

DROP TABLE IF EXISTS t_part_name_reuse SYNC;

CREATE TABLE t_part_name_reuse UUID '8f1e0d2c-3b4a-4596-8778-69aabbccddee' (a Int64, s String)
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_part_name_reuse VALUES (1, 'no');

SELECT 'one freshly inserted part';
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_part_name_reuse' AND active
    AND level = 0 AND min_block_number = max_block_number;

-- Primes the cache: nothing matches, so the granule is recorded as skippable for this predicate.
SELECT 'incarnation 1, nothing matches';
SELECT count() FROM t_part_name_reuse WHERE s = 'yes';
SELECT a, s FROM t_part_name_reuse WHERE s = 'yes';

SELECT 'an entry was written';
SELECT count() > 0 FROM system.query_condition_cache;

DROP TABLE t_part_name_reuse SYNC;

CREATE TABLE t_part_name_reuse UUID '8f1e0d2c-3b4a-4596-8778-69aabbccddee' (a Int64, s String)
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_part_name_reuse VALUES (2, 'yes');

-- Same shape as above, so this part carries the same name as the one the cache holds an entry for.
SELECT 'one freshly inserted part';
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_part_name_reuse' AND active
    AND level = 0 AND min_block_number = max_block_number;

SELECT 'incarnation 2, the row must be visible';
SELECT count() FROM t_part_name_reuse WHERE s = 'yes';
SELECT a, s FROM t_part_name_reuse WHERE s = 'yes';
SELECT count() FROM t_part_name_reuse WHERE s = 'yes' SETTINGS use_query_condition_cache = 0;

DROP TABLE t_part_name_reuse SYNC;
