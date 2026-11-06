-- A standalone expression - a `CHECK` constraint, a row policy, a `TTL` - is compiled by the machinery that
-- preceded the analyzer, and an `IN` subquery in it used to have its plan built by the interpreter that
-- preceded the analyzer as well. A `Distributed` table resolves its read through a query tree that
-- interpreter never builds, so reading one from there dereferenced a null query tree.

-- Under the stress profile a fuzzer copy of a `CREATE` or an ignored `DROP` can leave a table whose `TTL` reads
-- `in_subquery_dist` after `in_subquery_source` is gone, and the server then cannot load it on restart.
SET ast_fuzzer_any_query = 0;
SET ignore_drop_queries_probability = 0;

DROP TABLE IF EXISTS in_subquery_ttl;
DROP TABLE IF EXISTS in_subquery_merge;
DROP TABLE IF EXISTS in_subquery_child;
DROP TABLE IF EXISTS in_subquery_constraint_bare;
DROP TABLE IF EXISTS in_subquery_constraint;
DROP TABLE IF EXISTS in_subquery_dist;
DROP TABLE IF EXISTS in_subquery_source;

CREATE TABLE in_subquery_source (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO in_subquery_source VALUES (1), (2);

-- Two shards: a single-shard cluster is answered before the query tree is looked at.
CREATE TABLE in_subquery_dist (x UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), in_subquery_source);

-- The `IN` subquery of a `CHECK` constraint is executed while a row is inserted.
CREATE TABLE in_subquery_constraint (x UInt64, CONSTRAINT c CHECK x IN (SELECT x FROM in_subquery_dist))
ENGINE = MergeTree ORDER BY x;
INSERT INTO in_subquery_constraint VALUES (1), (2);
INSERT INTO in_subquery_constraint VALUES (100); -- { serverError VIOLATED_CONSTRAINT }
SELECT x FROM in_subquery_constraint ORDER BY x;

-- A bare table name on the right of `IN` is not allowed in a `CHECK` constraint; only a direct subquery is.
CREATE TABLE in_subquery_constraint_bare (x UInt64, CONSTRAINT c CHECK x IN in_subquery_dist)
ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- The `IN` subquery of a row policy is executed while a `Merge` table reads the table it is set on.
CREATE TABLE in_subquery_child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO in_subquery_child VALUES (1), (2), (3);
CREATE ROW POLICY OR REPLACE in_subquery_policy ON in_subquery_child USING x IN (SELECT x FROM in_subquery_dist) TO ALL;
CREATE TABLE in_subquery_merge (x UInt64) ENGINE = Merge(currentDatabase(), '^in_subquery_child$');
SELECT x FROM in_subquery_merge ORDER BY x;
DROP ROW POLICY in_subquery_policy ON in_subquery_child;

-- The `IN` subquery of a `TTL ... WHERE` is executed while a merge applies the `TTL`.
-- It is resolved outside of the current database, so the table is qualified.
CREATE TABLE in_subquery_ttl (x UInt64, d DateTime)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 SECOND WHERE x IN (SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.in_subquery_dist);
INSERT INTO in_subquery_ttl VALUES (1, now() - 100), (2, now() - 100), (3, now() - 100);
OPTIMIZE TABLE in_subquery_ttl FINAL;
SELECT x FROM in_subquery_ttl ORDER BY x;

DROP TABLE in_subquery_ttl;

DROP TABLE in_subquery_merge;
DROP TABLE in_subquery_child;
DROP TABLE in_subquery_constraint;
DROP TABLE in_subquery_dist;
DROP TABLE in_subquery_source;
