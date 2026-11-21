-- A projection `ORDER BY` tuple with an aliased element has to survive formatting.
-- If the tuple is unwrapped, `c0 AS x` is written at the top level, where the parser does not accept aliases.

SELECT formatQuery(formatQuery($$
    CREATE TABLE t
    (
        c0 Int32,
        c1 Int32,
        PROJECTION p (SELECT c0 ORDER BY (c0 AS x, c1))
    )
    ENGINE = MergeTree
    ORDER BY tuple()
$$)) = formatQuery($$
    CREATE TABLE t
    (
        c0 Int32,
        c1 Int32,
        PROJECTION p (SELECT c0 ORDER BY (c0 AS x, c1))
    )
    ENGINE = MergeTree
    ORDER BY tuple()
$$);

DROP TABLE IF EXISTS t_projection_order_by_tuple_alias;
CREATE TABLE t_projection_order_by_tuple_alias
(
    c0 Int32,
    c1 Int32,
    PROJECTION p (SELECT c0 ORDER BY (c0 AS x, c1))
)
ENGINE = MergeTree
ORDER BY tuple();

SHOW CREATE TABLE t_projection_order_by_tuple_alias FORMAT Null;
SELECT create_table_query != ''
FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_order_by_tuple_alias';

DETACH TABLE t_projection_order_by_tuple_alias SYNC;
ATTACH TABLE t_projection_order_by_tuple_alias;
SHOW CREATE TABLE t_projection_order_by_tuple_alias FORMAT Null;

DROP TABLE t_projection_order_by_tuple_alias;
