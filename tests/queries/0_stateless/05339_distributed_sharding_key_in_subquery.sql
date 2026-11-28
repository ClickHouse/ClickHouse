-- Tags: shard
-- A sharding key is built under the global context, so the tables it reads are qualified with the database.

CREATE TABLE src (c0 UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO src VALUES (1);
CREATE TABLE dst (c0 UInt8) ENGINE = Memory;
CREATE TABLE set_t (c0 UInt8) ENGINE = Set;
INSERT INTO set_t VALUES (1);
CREATE TABLE rmt (c0 UInt8) ENGINE = Remote('127.0.0.{1,2}', currentDatabase(), src);
CREATE TABLE als ENGINE = Alias(currentDatabase(), rmt);

CREATE TABLE d_subquery (c0 UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst, c0 IN (SELECT c0 FROM {CLICKHOUSE_DATABASE:Identifier}.src)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE d_table (c0 UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst, c0 IN {CLICKHOUSE_DATABASE:Identifier}.src); -- { serverError BAD_ARGUMENTS }
CREATE TABLE d_lambda (c0 UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst, arrayExists(x -> x IN (SELECT 1), [c0])); -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION remote('127.0.0.{1,2}', currentDatabase(), dst, c0 GLOBAL NOT IN {CLICKHOUSE_DATABASE:Identifier}.als) SELECT number % 2 FROM numbers(4); -- { serverError BAD_ARGUMENTS }
INSERT INTO FUNCTION remote('127.0.0.{1,2}', currentDatabase(), dst, c0 IN (SELECT c0 FROM {CLICKHOUSE_DATABASE:Identifier}.src)) SELECT number % 2 FROM numbers(4); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), dst, c0 IN {CLICKHOUSE_DATABASE:Identifier}.src) WHERE c0 = 1 SETTINGS optimize_skip_unused_shards = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE d_tuple (c0 UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst, c0 IN (1, 2));
CREATE TABLE d_set (c0 UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst, c0 IN {CLICKHOUSE_DATABASE:Identifier}.set_t);
INSERT INTO d_tuple SETTINGS distributed_foreground_insert = 1 SELECT number % 2 FROM numbers(4);
INSERT INTO d_set SETTINGS distributed_foreground_insert = 1 SELECT number % 2 FROM numbers(4);
SELECT count() FROM dst;

-- A `Set` table can change after rows are placed, so shard pruning by it needs the opt-in, as for `joinGet`.
SELECT count() FROM d_tuple WHERE c0 = 1 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1 FORMAT Null;
SELECT count() FROM d_set WHERE c0 = 1 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }
SELECT count() FROM d_set WHERE c0 = 1 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1, allow_nondeterministic_optimize_skip_unused_shards = 1 FORMAT Null;
