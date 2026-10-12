-- A projection materializes the columns it selects, so an ALIAS parent column becomes an ordinary
-- stored column of the projection, while inheriting the parent's expression for it. That expression is
-- written in terms of the parent's columns, and the projection only carries the ones it selects, so
-- implicit minmax indices - which index an ALIAS column over the expression behind it - have to cope
-- with a projection that does not carry every column the expression names.

DROP TABLE IF EXISTS t_04695_alias;

-- `ab_sum` is an expression ALIAS; the projection selects it but not `b`.
CREATE TABLE t_04695_alias
(
    id     UInt64,
    a      UInt32,
    b      UInt32,
    ab_sum UInt64 ALIAS a + b,
    PROJECTION p (SELECT ab_sum ORDER BY a)
)
ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_04695_alias (id, a, b) VALUES (1, 10, 5), (2, 1, 1);
SELECT 'alias', id, ab_sum FROM t_04695_alias ORDER BY id;

-- The parent still indexes the alias column, over its expression.
SELECT 'parent-index', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_04695_alias' AND name = 'auto_minmax_index_ab_sum';

-- The projection stores the alias column and can serve a read of it.
SELECT 'proj-read', ab_sum FROM t_04695_alias ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, enable_parallel_replicas = 0;

DROP TABLE t_04695_alias;

-- A MATERIALIZED parent column is stored in the parent too, and its default is still inherited.
DROP TABLE IF EXISTS t_04695_materialized;

CREATE TABLE t_04695_materialized
(
    id UInt64,
    a  UInt32,
    m  UInt64 MATERIALIZED a * 2,
    PROJECTION p (SELECT a, m ORDER BY a)
)
ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_04695_materialized (id, a) VALUES (1, 21), (2, 3);
SELECT 'materialized', id, m FROM t_04695_materialized ORDER BY id;

DROP TABLE t_04695_materialized;

-- Regression: under `optimize_respect_aliases = 0`, creating the alias-projection shape used to fail
-- with `UNKNOWN_IDENTIFIER`, because the implicit minmax index over the projection's inherited ALIAS
-- column was analyzed against the projection's own columns, and the alias expression names `b`, which
-- the projection does not carry. The implicit index is now skipped for inherited ALIAS columns, so the
-- projection metadata no longer depends on the session's alias handling.
SET optimize_respect_aliases = 0;

CREATE TABLE t_04695_alias_no_respect
(
    id     UInt64,
    a      UInt32,
    b      UInt32,
    ab_sum UInt64 ALIAS a + b,
    PROJECTION p (SELECT ab_sum ORDER BY a)
)
ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1;

SELECT 'create-no-respect-aliases-ok';

DROP TABLE t_04695_alias_no_respect;
