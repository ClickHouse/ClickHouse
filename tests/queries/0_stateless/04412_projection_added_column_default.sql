-- A column added via metadata-only ALTER ADD COLUMN ... DEFAULT is physically absent from a projection
-- part that was written before the column existed. Reading it through that projection must return the
-- column's DDL DEFAULT (-1), not the column type's default (0) -- matching the base-table read path.

DROP TABLE IF EXISTS t_proj_added_default;

CREATE TABLE t_proj_added_default
(
    a UInt64,
    b UInt64,
    id UInt64,
    PROJECTION p (SELECT * ORDER BY (a, b))
)
ENGINE = MergeTree ORDER BY (a, id);

-- Projection 'p' is materialized here, WITHOUT column c.
INSERT INTO t_proj_added_default SELECT 1, number % 10000, number FROM numbers(500000);

-- Metadata-only adds: existing projection part is not rewritten and still lacks c and d.
ALTER TABLE t_proj_added_default ADD COLUMN c Int64 DEFAULT -1;   -- with a DDL default
ALTER TABLE t_proj_added_default ADD COLUMN d Int64;             -- no default -> type default (0) is correct

-- WHERE matches the projection's ORDER BY (a, b), so the query is served by the projection.
-- (parallel replicas disabled: it does not interact with this single-node projection read and only adds flakiness.)
-- c: base-table read and projection read must both return the DDL default -1, not the type default 0.
SELECT 'c-base' AS path, c FROM t_proj_added_default WHERE a = 1 AND b = 4665 GROUP BY c SETTINGS optimize_use_projections = 0, enable_parallel_replicas = 0;
SELECT 'c-proj' AS path, c FROM t_proj_added_default WHERE a = 1 AND b = 4665 GROUP BY c SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, enable_parallel_replicas = 0;
-- d: no DDL default, so both reads must return the type default 0 (the fix must not inject a default here).
SELECT 'd-base' AS path, d FROM t_proj_added_default WHERE a = 1 AND b = 4665 GROUP BY d SETTINGS optimize_use_projections = 0, enable_parallel_replicas = 0;
SELECT 'd-proj' AS path, d FROM t_proj_added_default WHERE a = 1 AND b = 4665 GROUP BY d SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, enable_parallel_replicas = 0;

DROP TABLE t_proj_added_default;

-- An explicitly selected ALIAS parent column is materialized into an ordinary stored projection output
-- column; inheriting defaults must not turn it into a non-stored ALIAS inside the projection (which would
-- drop it from the projection's physical columns). Reading the alias column through the projection must
-- still return the computed value.
DROP TABLE IF EXISTS t_proj_alias;
CREATE TABLE t_proj_alias (a UInt64, b UInt64 ALIAS a + 1, PROJECTION p (SELECT a, b ORDER BY a)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_proj_alias (a) VALUES (1), (2);
SELECT 'alias' AS path, a, b FROM t_proj_alias ORDER BY a;
DROP TABLE t_proj_alias;
