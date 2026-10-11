-- Correlated scalar subqueries remain unsupported in ALTER UPDATE.

SET enable_analyzer = 1;
SET enable_lightweight_update = 1;
SET alter_update_mode = 'lightweight_force';
SET mutations_sync = 1;

DROP TABLE IF EXISTS t_correlated_alter_update_source;
DROP TABLE IF EXISTS t_correlated_alter_update_target;

CREATE TABLE t_correlated_alter_update_target
(
    id UInt64,
    value Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_correlated_alter_update_source
(
    id UInt64,
    value Int64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_correlated_alter_update_target VALUES (1, 0);
INSERT INTO t_correlated_alter_update_source VALUES (1, 10);

ALTER TABLE t_correlated_alter_update_target
UPDATE value =
(
    SELECT s.value
    FROM t_correlated_alter_update_source AS s
    WHERE s.id = t_correlated_alter_update_target.id
)
WHERE 1; --{serverError NOT_IMPLEMENTED}

SELECT value FROM t_correlated_alter_update_target;

DROP TABLE t_correlated_alter_update_source;
DROP TABLE t_correlated_alter_update_target;
