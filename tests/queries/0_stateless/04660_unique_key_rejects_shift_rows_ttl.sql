-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- The fast `MODIFY TTL` path records the materialization as an internal `SHIFT ROWS TTL BY <n> SECOND`
-- mutation, which is a distinct mutation type. `MergeTreeData::checkMutationIsPossible` rejects
-- `MATERIALIZE TTL` on `UNIQUE KEY` tables (TTL cannot be honored while merges are disabled there,
-- and the rewrite bypasses the dedup path), so it must reject the shift form as well - otherwise
-- enabling the optimization would reopen exactly the path the guard forbids. From SQL the outer
-- `MODIFY TTL` guard fires first (asserted below); the mutation-level check is the backstop for a
-- shift mutation that arrives from elsewhere, e.g. a replication log entry written by a replica
-- that has the optimization enabled.

SET allow_experimental_unique_key = 1;
SET enable_modify_ttl_by_extending_time_interval = 1;
SET allow_suspicious_ttl_expressions = 1;

DROP TABLE IF EXISTS uk_fast_ttl;

CREATE TABLE uk_fast_ttl (id UInt64, d DateTime('UTC'))
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

ALTER TABLE uk_fast_ttl MODIFY TTL d + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

-- The internal shift form is not reachable from SQL at all.
ALTER TABLE uk_fast_ttl SHIFT ROWS TTL BY 100 SECOND; -- { serverError BAD_ARGUMENTS }
ALTER TABLE uk_fast_ttl MATERIALIZE TTL; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE uk_fast_ttl;

SELECT 'ok' AS step;
