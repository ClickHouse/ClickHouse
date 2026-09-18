-- `ALTER TABLE ... MODIFY PROJECTION` accepts a body restated with comparison-insensitive spelling,
-- but it must keep the stored body byte-for-byte and swap only the `WITH SETTINGS` clause: the
-- serialized `projections` metadata field is compared as text by replicas that do not know about the
-- AST comparison, so a respelled body would drift and break them with `METADATA_MISMATCH`.

DROP TABLE IF EXISTS t_modify_projection_stored_body;

CREATE TABLE t_modify_projection_stored_body
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT k, sum(v) GROUP BY k) WITH SETTINGS (index_granularity = 64)
)
ENGINE = MergeTree ORDER BY k;

SELECT 'initial', query FROM system.projections
WHERE database = currentDatabase() AND table = 't_modify_projection_stored_body';

-- Function name in a different case, and redundant parentheses: the same body as an AST.
ALTER TABLE t_modify_projection_stored_body MODIFY PROJECTION p (SELECT k, SUM((v)) GROUP BY (k)) WITH SETTINGS (index_granularity = 128);

-- The stored body is still the original spelling, only the settings changed.
SELECT 'restated', query, settings FROM system.projections
WHERE database = currentDatabase() AND table = 't_modify_projection_stored_body';

SELECT 'create', create_table_query LIKE '%(SELECT k, sum(v) GROUP BY k) WITH SETTINGS (index_granularity = 128)%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_modify_projection_stored_body';

-- Dropping the `WITH SETTINGS` clause also keeps the body.
ALTER TABLE t_modify_projection_stored_body MODIFY PROJECTION p (SELECT k, SUM(v) GROUP BY k);

SELECT 'no settings', query, settings FROM system.projections
WHERE database = currentDatabase() AND table = 't_modify_projection_stored_body';

DROP TABLE t_modify_projection_stored_body;
