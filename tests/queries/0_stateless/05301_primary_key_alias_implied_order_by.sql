-- Tags: no-shared-merge-tree
-- Tag no-shared-merge-tree: SharedMergeTree, like ReplicatedMergeTree, rejects an aliased PRIMARY KEY at CREATE

-- A table with PRIMARY KEY and no ORDER BY stores the PRIMARY KEY as its ORDER BY too.
-- An alias on the PRIMARY KEY must not be stored in that ORDER BY, or the table definition cannot be read back.

DROP TABLE IF EXISTS t_pk_alias;
DROP TABLE IF EXISTS t_pk_alias_expr;
DROP TABLE IF EXISTS mv_pk_alias;
DROP TABLE IF EXISTS t_src;

CREATE TABLE t_pk_alias (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 AS a);
INSERT INTO t_pk_alias VALUES (1), (2), (3);
SHOW CREATE TABLE t_pk_alias FORMAT Null;
SELECT sorting_key, primary_key FROM system.tables WHERE database = currentDatabase() AND name = 't_pk_alias';
DETACH TABLE t_pk_alias SYNC;
ATTACH TABLE t_pk_alias;
SELECT count() FROM t_pk_alias;

CREATE TABLE t_pk_alias_expr (c0 Int64) ENGINE = MergeTree PRIMARY KEY (c0 * 2 AS a);
INSERT INTO t_pk_alias_expr VALUES (1), (2), (3);
DETACH TABLE t_pk_alias_expr SYNC;
ATTACH TABLE t_pk_alias_expr;
SELECT count() FROM t_pk_alias_expr;

CREATE TABLE t_src (x Int64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_pk_alias ENGINE = MergeTree PRIMARY KEY (x AS a) AS SELECT x FROM t_src;
INSERT INTO t_src VALUES (1), (2);
SHOW CREATE TABLE mv_pk_alias FORMAT Null;
SELECT count() FROM mv_pk_alias;

DROP TABLE mv_pk_alias;
DROP TABLE t_src;
DROP TABLE t_pk_alias_expr;
DROP TABLE t_pk_alias;
