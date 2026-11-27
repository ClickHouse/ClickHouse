-- Tags: no-parallel, no-replicated-database
-- - no-parallel - due to usage of fail points, and `materialized_views_populate_atomically` is on by
--   default, so a concurrent `CREATE MATERIALIZED VIEW ... POPULATE` of another test would hit them too.
-- - no-replicated-database - in a `Replicated` database the `CREATE` is an entry of the replicated DDL log
--   and the failed atomic cut is not rolled back locally (that would diverge this replica from the ones
--   where the same entry succeeded), so the view is left behind there.

-- A failure of the atomic cut of `CREATE MATERIALIZED VIEW ... POPULATE` (e.g. an exclusive-lock timeout
-- on a busy source) happens after the view was already created and started, but possibly before the view
-- was subscribed to its source. It must drop the just-created view: otherwise the failed CREATE would
-- leave behind a view that future inserts silently never populate.

DROP TABLE IF EXISTS src_04653;
DROP TABLE IF EXISTS mv_04653;

CREATE TABLE src_04653 (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO src_04653 SELECT number FROM numbers(10);

SYSTEM ENABLE FAILPOINT atomic_populate_fail_before_subscription;

CREATE MATERIALIZED VIEW mv_04653 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04653; -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT atomic_populate_fail_before_subscription;

-- The failed CREATE left nothing behind: neither the view nor a subscription of the source.
SELECT 'view exists after failed CREATE:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04653';
SELECT 'source has dependents after failed CREATE:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'src_04653' AND notEmpty(dependencies_table);

-- A retry succeeds and the view works: it has the populated rows and receives new inserts.
CREATE MATERIALIZED VIEW mv_04653 ENGINE = MergeTree ORDER BY n POPULATE AS SELECT n FROM src_04653;
INSERT INTO src_04653 SELECT number + 10 FROM numbers(5);
SELECT 'rows in view after retry and insert:', count() FROM mv_04653;

DROP TABLE mv_04653;
DROP TABLE src_04653;
