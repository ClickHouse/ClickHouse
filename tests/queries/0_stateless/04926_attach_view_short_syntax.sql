-- Tags: no-replicated-database
-- no-replicated-database: the short ATTACH VIEW is rejected in a Replicated database.

CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t VALUES (1);

CREATE VIEW v AS SELECT k FROM t;
DETACH VIEW v;
ATTACH VIEW v;
SELECT * FROM v;

CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY k AS SELECT k FROM t;
DETACH VIEW mv;
ATTACH MATERIALIZED VIEW mv;
SELECT count() FROM mv;

DETACH VIEW mv;
ATTACH VIEW mv;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'mv';

DETACH VIEW v;
ATTACH VIEW IF NOT EXISTS v;
ATTACH VIEW IF NOT EXISTS v;
SELECT * FROM v;

DETACH VIEW v;
ATTACH VIEW v UUID '00000000-0000-0000-0000-000000000001'; -- { serverError BAD_ARGUMENTS }
ATTACH VIEW v;

DETACH TABLE t;
ATTACH VIEW t; -- { serverError INCORRECT_QUERY }
ATTACH TABLE t;
SELECT * FROM t;

ATTACH TEMPORARY VIEW tv; -- { serverError SYNTAX_ERROR }

ATTACH OR REPLACE VIEW v; -- { clientError SYNTAX_ERROR }
ATTACH SQL SECURITY INVOKER VIEW v; -- { clientError SYNTAX_ERROR }
ATTACH DEFINER = default VIEW v; -- { clientError SYNTAX_ERROR }

SELECT formatQuery('ATTACH MATERIALIZED VIEW mv');
SELECT formatQuery('ATTACH VIEW v ON CLUSTER test_shard_localhost');

DETACH VIEW v;
ATTACH VIEW v ON CLUSTER test_shard_localhost; -- { serverError INCORRECT_QUERY }
ATTACH MATERIALIZED VIEW v ON CLUSTER test_shard_localhost; -- { serverError INCORRECT_QUERY }
SET distributed_ddl_entry_format_version = 2;
ATTACH VIEW v ON CLUSTER test_shard_localhost; -- { serverError INCORRECT_QUERY }
SET distributed_ddl_entry_format_version = DEFAULT;
ATTACH TABLE v;
SELECT * FROM v;

DROP VIEW v;
DROP VIEW mv;
DROP TABLE t;
