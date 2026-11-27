-- Tags: no-replicated-database
-- - no-replicated-database - in a `Replicated` database the `CREATE` is an entry of the replicated DDL log
--   and the failed atomic population is not rolled back locally (that would diverge this replica from the
--   ones where the same entry succeeded), so the view is left behind there.

-- A runtime failure of the population of `CREATE MATERIALIZED VIEW ... POPULATE` happens after the view was
-- already created, subscribed to its source and the population pipeline was built. The population executes
-- eagerly inside the rollback scope of `fillMaterializedViewAtomically`, so such a failure must also drop
-- the just-created view: otherwise the failed CREATE would leave behind a subscribed view with partial
-- data, and a retry would fail with `TABLE_ALREADY_EXISTS`.

DROP TABLE IF EXISTS src_04759;
DROP TABLE IF EXISTS mv_04759;

CREATE TABLE src_04759 (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO src_04759 SELECT number FROM numbers(10);

-- The population pipeline is built successfully; `throwIf` fires only while it runs, on the source rows.
CREATE MATERIALIZED VIEW mv_04759 ENGINE = MergeTree ORDER BY n POPULATE
    AS SELECT n, throwIf(n = 5) AS fail FROM src_04759; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The failed CREATE left nothing behind: neither the view nor a subscription of the source.
SELECT 'view exists after failed CREATE:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04759';
SELECT 'source has dependents after failed CREATE:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'src_04759' AND notEmpty(dependencies_table);

-- A retry (with a SELECT that does not fail) succeeds and the view works: it has the populated rows and
-- receives new inserts.
CREATE MATERIALIZED VIEW mv_04759 ENGINE = MergeTree ORDER BY n POPULATE
    AS SELECT n, throwIf(n = 1000) AS fail FROM src_04759;
INSERT INTO src_04759 SELECT number + 10 FROM numbers(5);
SELECT 'rows in view after retry and insert:', count() FROM mv_04759;

DROP TABLE mv_04759;
DROP TABLE src_04759;
