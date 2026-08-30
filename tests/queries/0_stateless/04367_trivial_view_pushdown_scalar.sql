-- The trivial-view pushdown reads the underlying Distributed table through effective_context, a
-- copy of the caller/SQL-security context with result-size limits disabled. StorageDistributed
-- rewrites the shard query using table_expression_query_info.planner_context's query context, and
-- ReplaceLongConstWithScalar registers the resulting scalar there. Because Context::createCopy
-- snapshots the scalar map by value, the planner context and effective_context could diverge, so a
-- shard query containing __getScalar('<hash>') could be sent without the matching scalar
-- ("Scalar doesn't exist"). The fix points planner_context at effective_context, so the scalar is
-- registered in the same context that sends it. A small optimize_const_name_size forces a modest
-- literal through the long-const-to-scalar rewrite.
--
-- Tags: distributed

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;
SET optimize_trivial_view_pushdown_to_distributed = 1;
SET optimize_const_name_size = 8;

DROP TABLE IF EXISTS 04367_local;
DROP TABLE IF EXISTS 04367_dist;
DROP VIEW IF EXISTS 04367_view;

CREATE TABLE 04367_local (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE 04367_dist AS 04367_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04367_local);
CREATE VIEW 04367_view AS SELECT id FROM 04367_dist;

INSERT INTO 04367_dist VALUES (1), (2);
SYSTEM FLUSH DISTRIBUTED 04367_dist;

-- The long literal is rewritten to __getScalar in the shard query; the scalar must travel with it.
SELECT 'a_long_constant_value' AS c, id FROM 04367_view ORDER BY id;

DROP VIEW 04367_view;
DROP TABLE 04367_dist;
DROP TABLE 04367_local;
