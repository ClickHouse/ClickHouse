-- Tags: no-replicated-database
-- - no-replicated-database - in a `Replicated` database the `CREATE` is an entry of the replicated DDL log
--   and the failed atomic population is not rolled back locally (that would diverge this replica from the
--   ones where the same entry succeeded), so the view is left behind there.

-- A runtime failure of the population of `CREATE MATERIALIZED VIEW ... TO target POPULATE` must drop the
-- just-created view, like for the plain ENGINE form. But the pre-existing target table is not ours to roll
-- back: it must survive with its pre-existing rows intact, and rows the failed population already appended
-- to it stay there, exactly as after a failed `INSERT ... SELECT` into that table - so a retry may insert
-- them again. This test pins down that narrowed rollback contract for an external target.

DROP TABLE IF EXISTS src_04815;
DROP TABLE IF EXISTS target_04815;
DROP TABLE IF EXISTS mv_04815;

CREATE TABLE src_04815 (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO src_04815 SELECT number FROM numbers(10);

-- The target pre-exists and already has data (marker rows the backfill cannot produce).
CREATE TABLE target_04815 (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO target_04815 VALUES (1000000), (1000001);

-- The population pipeline is built successfully; `throwIf` fires only while it runs, on the source rows.
CREATE MATERIALIZED VIEW mv_04815 TO target_04815 POPULATE
    AS SELECT n + throwIf(n = 5) AS n FROM src_04815; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The failed CREATE left behind nothing of what it created: neither the view nor a subscription of the
-- source. The target table it did not create is untouched by the rollback: it still exists and keeps its
-- pre-existing rows. (Rows the failed population managed to append before the failure may remain in the
-- target; they are indistinguishable from a failed `INSERT ... SELECT`, so they are not asserted here.)
SELECT 'view exists after failed CREATE:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04815';
SELECT 'source has dependents after failed CREATE:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'src_04815' AND notEmpty(dependencies_table);
SELECT 'target exists after failed CREATE:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'target_04815';
SELECT 'pre-existing target rows after failed CREATE:', count() FROM target_04815 WHERE n >= 1000000;

-- A retry (with a SELECT that does not fail) succeeds: the backfilled rows are in the target and new
-- inserts into the source flow through the view. `countDistinct` tolerates remnants of the failed
-- backfill, which may duplicate some of the rows.
CREATE MATERIALIZED VIEW mv_04815 TO target_04815 POPULATE
    AS SELECT n + throwIf(n = 1000) AS n FROM src_04815;
SELECT 'distinct backfilled rows after retry:', countDistinct(n) FROM target_04815 WHERE n < 1000000;
INSERT INTO src_04815 SELECT number + 10 FROM numbers(5);
SELECT 'distinct rows after retry and insert:', countDistinct(n) FROM target_04815 WHERE n < 1000000;
SELECT 'pre-existing target rows after retry:', count() FROM target_04815 WHERE n >= 1000000;

DROP TABLE mv_04815;
DROP TABLE target_04815;
DROP TABLE src_04815;
