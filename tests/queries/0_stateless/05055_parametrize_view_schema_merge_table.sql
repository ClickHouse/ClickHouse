-- A `Merge` table cannot supply parameter values, so a parameterized view matched by its regexp
-- must be rejected with `STORAGE_REQUIRES_PARAMETER`. This must not be inferred from an empty
-- column list: a parameterized view with an explicitly declared schema does report columns.

DROP TABLE IF EXISTS 05055_merge;
DROP TABLE IF EXISTS 05055_t;
DROP VIEW IF EXISTS 05055_pv_no_schema;
DROP VIEW IF EXISTS 05055_pv_declared;

CREATE TABLE 05055_t (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO 05055_t VALUES (1);

SET use_declared_schema_for_parameterized_views = 1;

-- Without a declared schema.
CREATE VIEW 05055_pv_no_schema AS SELECT number AS n FROM numbers({upper_bound:UInt64});
-- With a declared schema, which is latched into the stored definition and therefore exposed.
CREATE VIEW 05055_pv_declared (n UInt64) AS SELECT number AS n FROM numbers({upper_bound:UInt64});

CREATE TABLE 05055_merge (n UInt64) ENGINE = Merge(currentDatabase(), '05055_(t|pv_.*)');

-- { echoOn }

SHOW COLUMNS IN 05055_pv_declared;

SELECT n FROM 05055_merge ORDER BY n; -- { serverError STORAGE_REQUIRES_PARAMETER }

-- The declared schema is latched at `CREATE` time, so the rejection does not depend on the
-- query-time value of the setting either.
SELECT n FROM 05055_merge SETTINGS use_declared_schema_for_parameterized_views = 0; -- { serverError STORAGE_REQUIRES_PARAMETER }

-- { echoOff }

DROP VIEW 05055_pv_declared;

-- { echoOn }

-- The view without a declared schema is rejected the same way.
SELECT n FROM 05055_merge ORDER BY n; -- { serverError STORAGE_REQUIRES_PARAMETER }

-- { echoOff }

DROP VIEW 05055_pv_no_schema;

-- { echoOn }

-- With no parameterized view left, the `Merge` table reads the plain table.
SELECT n FROM 05055_merge ORDER BY n;

-- { echoOff }

DROP TABLE 05055_merge;
DROP TABLE 05055_t;
