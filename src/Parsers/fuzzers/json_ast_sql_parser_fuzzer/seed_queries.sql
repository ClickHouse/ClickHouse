-- Seed queries for json_ast_sql_parser_fuzzer. One statement per line; lines starting with `--` are skipped.
-- `generate_seed_corpus.py` turns every statement into a JSON AST with `parseQueryToJSON` and then into
-- a binary protobuf seed with `json_ast_seed_converter`. Keep the list small and coverage-oriented: one or
-- two statements per syntax feature, taken from or modelled on tests/queries/0_stateless.

-- SELECT basics, expressions, literals
SELECT 1
SELECT a, b + 1 AS c, -x, NOT y, a AND b OR c, a IS NULL, a IS NOT NULL, a BETWEEN 1 AND 2, a LIKE '%x%', a NOT IN (1, 2, 3) FROM t
SELECT number, toString(number) AS s, number * 2 + 1, intDiv(number, 3), number % 2 = 0 FROM numbers(10) WHERE number > 3 ORDER BY number DESC, s ASC NULLS FIRST LIMIT 3 OFFSET 1
SELECT 'string', 'it''s', 42, -7, 3.14, 1e10, -inf, nan, NULL, true, false, 0x1F, toDecimal32(1.5, 2), toUUID('00000000-0000-0000-0000-000000000000'), toDate('2024-01-01'), toDateTime64('2024-01-01 00:00:00.123', 3)
SELECT CASE WHEN a > 1 THEN 'big' WHEN a = 1 THEN 'one' ELSE 'small' END, CASE a WHEN 1 THEN 'x' ELSE 'y' END, if(a, 1, 2), multiIf(a, 1, b, 2, 3) FROM t
SELECT CAST(a AS Nullable(UInt32)), a::String, CAST(1, 'UInt8'), toTypeName([1, 2]), extract(s, '\\d+'), position(s, 'x'), s[1], t.1, m['key'] FROM t
SELECT DISTINCT a, b FROM t
SELECT DISTINCT ON (a) a, b FROM t ORDER BY a
SELECT * EXCEPT (a, b) REPLACE (c + 1 AS c) APPLY (sum) FROM t
SELECT COLUMNS('^a.*') APPLY (x -> x + 1), t.* EXCEPT (z), COLUMNS(a, b) FROM t
SELECT count(), count(DISTINCT a), countIf(a > 1), sum(a) FILTER (WHERE b > 0), uniqCombined(12)(a), quantiles(0.5, 0.9)(a), argMax(a, b), any(a) RESPECT NULLS, groupArray(3)(a) FROM t GROUP BY b HAVING count() > 1
SELECT a, sum(b) FROM t GROUP BY a WITH TOTALS ORDER BY a WITH FILL FROM 1 TO 10 STEP 2 INTERPOLATE (b AS b + 1)
SELECT a, b, sum(c) FROM t GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY a, b
SELECT a, sum(c) FROM t GROUP BY a WITH ROLLUP SETTINGS group_by_use_nulls = 1
SELECT a, sum(c) FROM t GROUP BY ALL ORDER BY ALL LIMIT 5 WITH TIES
SELECT a, b FROM t ORDER BY a LIMIT 1 BY a, b
SELECT a FROM t PREWHERE b > 0 WHERE c > 0 QUALIFY row_number() OVER (PARTITION BY a) = 1
SELECT number FROM numbers(10) SAMPLE 0.5 OFFSET 0.25
SELECT * FROM t FINAL WHERE a = 1 SETTINGS max_threads = 1, optimize_read_in_order = 0
SELECT 1 FORMAT JSONEachRow
SELECT 1 INTO OUTFILE 'out.tsv' TRUNCATE COMPRESSION 'gzip' LEVEL 3 FORMAT TSV
SELECT {param:UInt32}, {ident:Identifier} FROM {tbl:Identifier}
SELECT * FROM t WHERE a IN (SELECT a FROM t2) AND b GLOBAL IN (1, 2) AND (c, d) IN ((1, 2), (3, 4))
SELECT (SELECT max(a) FROM t) AS m, exists(SELECT 1 FROM t WHERE a = 0)
SELECT number FROM numbers(3) SETTINGS max_block_size = 1 UNION ALL SELECT 4 UNION DISTINCT SELECT 5
SELECT 1 EXCEPT SELECT 2 INTERSECT SELECT 3
SELECT * FROM (SELECT 1 UNION ALL SELECT 2) ORDER BY 1 LIMIT 1
SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2) AS u WHERE a > 0

-- WITH / CTE
WITH 1 AS one, a + one AS b SELECT b FROM t
WITH cte AS (SELECT a, count() AS c FROM t GROUP BY a) SELECT * FROM cte WHERE c > 1
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT * FROM r
WITH (SELECT max(a) FROM t) AS m SELECT a / m FROM t
WITH x -> x * 2 AS dbl SELECT dbl(a) FROM t
WITH cte(a, b) AS (SELECT 1, 2) SELECT b FROM cte

-- JOINs, ARRAY JOIN, table functions
SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t1.a = t2.a AND t1.b < t2.b
SELECT * FROM t1 LEFT OUTER JOIN t2 USING (a, b)
SELECT * FROM t1 GLOBAL ANY LEFT JOIN t2 ON t1.k = t2.k
SELECT * FROM t1 ASOF LEFT JOIN t2 ON t1.k = t2.k AND t1.ts >= t2.ts
SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.a = t2.a RIGHT ANTI JOIN t3 ON t2.b = t3.b
SELECT * FROM t1 CROSS JOIN t2
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a
SELECT * FROM t1 SEMI LEFT JOIN t2 ON t1.a = t2.a
SELECT * FROM t1 PASTE JOIN t2
SELECT a, arr_elem FROM t ARRAY JOIN arr AS arr_elem, arrayEnumerate(arr) AS idx
SELECT * FROM t LEFT ARRAY JOIN nested.a, nested.b
SELECT * FROM numbers(10) AS n JOIN (SELECT number AS x FROM numbers(5)) AS s ON n.number = s.x
SELECT * FROM remote('127.0.0.{1,2}', default, t) WHERE a = 1
SELECT * FROM cluster('test_cluster', view(SELECT 1))
SELECT * FROM merge(default, '^t_') FINAL
SELECT * FROM file('data.csv', 'CSV', 'a UInt32, b String')
SELECT * FROM s3('https://bucket.s3.amazonaws.com/file.parquet', 'Parquet')
SELECT * FROM generateRandom('a UInt8, b String', 1, 10, 2) LIMIT 5
SELECT * FROM values('a UInt8, b String', (1, 'x'), (2, 'y'))
SELECT * FROM t AS alias(x, y)
SELECT * FROM db.t STREAM

-- Lambdas, arrays, tuples, maps, higher-order functions
SELECT arrayMap(x -> x * 2, [1, 2, 3]), arrayFilter((x, i) -> i % 2 = 0, arr, arrayEnumerate(arr)), arrayReduce('sum', [1, 2]), arraySort(x -> -x, arr) FROM t
SELECT [1, 2, [3, 4]], [], array(), tuple(1, 'a'), (1, 'a', [1]), tuple(1 AS x, 2 AS y), (1)
SELECT map('a', 1, 'b', 2), map(), mapKeys(m), m['a'], mapApply((k, v) -> (k, v + 1), m), mapFromArrays(['a'], [1]) FROM t
SELECT arrayJoin([1, 2, 3]) AS x, arrayExists(y -> y > x, [1, 2]), arrayFold((acc, x) -> acc + x, [1, 2], toUInt64(0))
SELECT tupleElement(t, 'a'), t.a, t.1, untuple(t), tupleNames(t) FROM tbl
SELECT [(1, 'a'), (2, 'b')]::Array(Tuple(UInt8, String)), CAST([[1]] AS Array(Array(UInt8)))

-- Window functions
SELECT a, sum(b) OVER (PARTITION BY a ORDER BY c ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t
SELECT a, row_number() OVER w, lagInFrame(b, 1, 0) OVER w, dense_rank() OVER (ORDER BY a) FROM t WINDOW w AS (PARTITION BY a ORDER BY b RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
SELECT count() OVER (), max(a) OVER (ORDER BY b DESC NULLS LAST GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t
SELECT exponentialTimeDecayedAvg(1)(value, time) OVER (ORDER BY time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t
SELECT nth_value(a, 2) OVER (PARTITION BY b ORDER BY c ASC) AS nv, first_value(a) RESPECT NULLS OVER (ORDER BY c) FROM t

-- Data types in CREATE TABLE and ClickHouse DDL
CREATE TABLE t (`id` UInt64, `s` String DEFAULT '', `n` Nullable(Int32), `lc` LowCardinality(String), `d` DateTime64(3, 'UTC'), `arr` Array(Array(UInt8)), `tup` Tuple(a UInt8, b String), `m` Map(String, UInt64), `e` Enum8('a' = 1, 'b' = 2), `f` FixedString(16), `dec` Decimal(18, 4), `ip` IPv6, `u` UUID, `b` Bool, `v` Variant(UInt8, String), `dyn` Dynamic, `j` JSON) ENGINE = MergeTree ORDER BY id
CREATE TABLE IF NOT EXISTS db.t ON CLUSTER c (`a` UInt32 CODEC(Delta, ZSTD(3)), `b` String MATERIALIZED toString(a), `c` UInt8 ALIAS a + 1, `d` Date EPHEMERAL today(), `e` UInt64 STATISTICS(tdigest) COMMENT 'col' TTL d + INTERVAL 1 DAY, INDEX idx b TYPE bloom_filter(0.01) GRANULARITY 4, PROJECTION p (SELECT a, count() GROUP BY a), CONSTRAINT chk CHECK a > 0) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}') PARTITION BY toYYYYMM(d) PRIMARY KEY a ORDER BY (a, b) SAMPLE BY a TTL d + INTERVAL 1 MONTH DELETE, d + INTERVAL 1 WEEK TO VOLUME 'cold' SETTINGS index_granularity = 8192 COMMENT 'table'
CREATE TABLE t (`a` UInt8, `b` Nested(x UInt32, y String)) ENGINE = Memory
CREATE TABLE t (`a` UInt8, `s` SimpleAggregateFunction(sum, UInt64), `st` AggregateFunction(uniq, String)) ENGINE = AggregatingMergeTree ORDER BY a
CREATE TEMPORARY TABLE tmp (`x` UInt8) ENGINE = Memory
CREATE TABLE t2 AS t ENGINE = Log
CREATE TABLE t3 ENGINE = MergeTree ORDER BY tuple() AS SELECT number AS n FROM numbers(10)
CREATE OR REPLACE TABLE t (`a` UInt8) ENGINE = TinyLog
CREATE TABLE t (`a` UInt8) ENGINE = Distributed('cluster', 'db', 'local', rand())
CREATE TABLE t (`a` UInt8) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'topic', kafka_format = 'JSONEachRow'
CREATE TABLE t (`a` UInt8 AUTO_INCREMENT, `b` UInt8 NOT NULL, `c` UInt8 NULL, `d` UInt8 DEFAULT 1) ENGINE = Memory
CREATE TABLE {tbl:Identifier} (`x` UInt8) ENGINE = Memory
CREATE TABLE t CLONE AS src
CREATE TABLE t (`id` UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY id
CREATE MATERIALIZED VIEW mv TO dst AS SELECT a, count() FROM src GROUP BY a
CREATE MATERIALIZED VIEW mv ENGINE = SummingMergeTree ORDER BY a POPULATE AS SELECT a, sum(b) AS b FROM src GROUP BY a
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR OFFSET 10 MINUTE RANDOMIZE FOR 5 MINUTE DEPENDS ON other APPEND TO dst EMPTY AS SELECT 1
CREATE VIEW v DEFINER = user SQL SECURITY DEFINER AS SELECT 1
CREATE VIEW pv AS SELECT * FROM t WHERE a = {p:UInt8}
CREATE DICTIONARY d (`k` UInt64, `v` String DEFAULT 'x' INJECTIVE) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src' DB 'db')) LIFETIME(MIN 0 MAX 300) LAYOUT(HASHED()) SETTINGS(max_threads = 1) COMMENT 'dict'
CREATE DICTIONARY d (`k` UInt64, `s` Date, `e` Date, `v` UInt8 HIERARCHICAL IS_OBJECT_ID) PRIMARY KEY k SOURCE(FILE(PATH '/tmp/f.csv' FORMAT 'CSV')) LIFETIME(0) LAYOUT(RANGE_HASHED()) RANGE(MIN s MAX e)
CREATE DATABASE db ENGINE = Atomic COMMENT 'c'
CREATE DATABASE db ENGINE = Replicated('/clickhouse/db', '{shard}', '{replica}') SETTINGS max_broken_tables_ratio = 1
CREATE DATABASE db ENGINE = MaterializedMySQL('host:3306', 'db', 'user', 'pass') TABLE OVERRIDE t (COLUMNS (`x` UInt8) PARTITION BY x ORDER BY x)
CREATE FUNCTION f AS (x, y) -> x + y
CREATE OR REPLACE FUNCTION f AS x -> x * 2
CREATE INDEX idx ON t (col) TYPE minmax GRANULARITY 1
CREATE TABLE t (`a` UInt8) ENGINE = MergeTree ORDER BY a SETTINGS storage_policy = 'default'

-- ALTER
ALTER TABLE t ADD COLUMN IF NOT EXISTS c UInt64 DEFAULT 0 CODEC(LZ4) AFTER b, DROP COLUMN IF EXISTS d, MODIFY COLUMN e String COMMENT 'x', RENAME COLUMN f TO g, COMMENT COLUMN h 'c', CLEAR COLUMN i IN PARTITION 1
ALTER TABLE t ON CLUSTER c MODIFY ORDER BY (a, b), MODIFY SAMPLE BY a, MODIFY COMMENT 'c'
ALTER TABLE t MODIFY TTL d + INTERVAL 1 DAY
ALTER TABLE t MODIFY SETTING index_granularity = 1024
ALTER TABLE t RESET SETTING storage_policy
ALTER TABLE t ADD INDEX idx a TYPE minmax GRANULARITY 2 AFTER b, DROP INDEX idx2, MATERIALIZE INDEX idx3 IN PARTITION 1, CLEAR INDEX idx4 IN PARTITION 2
ALTER TABLE t ADD PROJECTION p (SELECT a, b ORDER BY b), DROP PROJECTION p2, MATERIALIZE PROJECTION p3, CLEAR PROJECTION p4 IN PARTITION 1
ALTER TABLE t ADD CONSTRAINT c CHECK a > 0
ALTER TABLE t DROP CONSTRAINT c2
ALTER TABLE t ADD STATISTICS a TYPE tdigest
ALTER TABLE t DROP STATISTICS b
ALTER TABLE t MATERIALIZE STATISTICS c
ALTER TABLE t CLEAR STATISTICS d IN PARTITION 1
ALTER TABLE t DROP PARTITION 202401, DROP PART 'all_1_1_0', FORGET PARTITION 1
ALTER TABLE t DROP DETACHED PARTITION ID 'x' SETTINGS allow_drop_detached = 1
ALTER TABLE t ATTACH PARTITION (1, 'a') FROM src, ATTACH PART 'p' FROM '/path', MOVE PARTITION 1 TO DISK 'd', MOVE PARTITION 2 TO VOLUME 'v', MOVE PARTITION 3 TO TABLE dst, MOVE PART 'p' TO SHARD '/s', REPLACE PARTITION 4 FROM src2
ALTER TABLE t FETCH PARTITION 1 FROM '/zk/path', FETCH PART 'p' FROM '/zk/path', FREEZE PARTITION 2 WITH NAME 'b', FREEZE WITH NAME 'all', UNFREEZE PARTITION 3 WITH NAME 'b', UNFREEZE WITH NAME 'all'
ALTER TABLE t DELETE WHERE a = 1, UPDATE b = b + 1, c = 'x' IN PARTITION 1 WHERE d = 2, APPLY DELETED MASK IN PARTITION 1, MATERIALIZE COLUMN c IN PARTITION 1, MATERIALIZE TTL, REMOVE TTL
ALTER TABLE t DELETE IN PARTITION (1, 'a'), (2, 'b') WHERE x = 1
ALTER TABLE t MODIFY COLUMN a REMOVE DEFAULT, MODIFY COLUMN d UInt8 FIRST
ALTER TABLE t MODIFY COLUMN b MODIFY SETTING max_compress_block_size = 1
ALTER TABLE t MODIFY COLUMN c RESET SETTING max_compress_block_size
ALTER TABLE t (UPDATE x = 1 WHERE 1)
ALTER TABLE mv MODIFY QUERY SELECT a FROM src
ALTER TABLE mv MODIFY REFRESH EVERY 2 HOUR
ALTER TABLE v MODIFY SQL SECURITY INVOKER
ALTER DATABASE db MODIFY SETTING max_broken_tables_ratio = 1
ALTER DATABASE db MODIFY COMMENT 'c'

-- INSERT
INSERT INTO t (a, b) SELECT number, toString(number) FROM numbers(10)
INSERT INTO db.t SELECT * FROM src WHERE a > 0 SETTINGS max_insert_threads = 2
INSERT INTO FUNCTION file('out.parquet', 'Parquet') SELECT * FROM t
INSERT INTO t (* EXCEPT (b)) SELECT a FROM src
INSERT INTO TABLE FUNCTION remote('host', db, t) (a) SELECT 1
INSERT INTO t FROM INFILE 'data.csv' COMPRESSION 'gzip' FORMAT CSV
INSERT INTO t SELECT * FROM src SETTINGS async_insert = 1

-- Other statements
SET max_threads = 8, allow_experimental_analyzer = 1, dialect = 'clickhouse'
USE db
DROP TABLE IF EXISTS db.t SYNC
DROP TABLE IF EMPTY t
DROP DATABASE IF EXISTS db ON CLUSTER c
DROP DICTIONARY d
DROP VIEW v
DROP INDEX idx ON t
DETACH TABLE t PERMANENTLY
ATTACH TABLE t FROM '/path' (`a` UInt8) ENGINE = Memory
TRUNCATE TABLE IF EXISTS t
TRUNCATE ALL TABLES FROM db LIKE 'x%' SYNC
RENAME TABLE a TO b, db.c TO db.d
RENAME DATABASE a TO b
EXCHANGE TABLES a AND b
EXCHANGE DICTIONARIES a AND b
OPTIMIZE TABLE t ON CLUSTER c PARTITION 1 FINAL DEDUPLICATE BY a, b
OPTIMIZE TABLE t FINAL CLEANUP
KILL QUERY WHERE query_id = 'x' SYNC
KILL MUTATION ON CLUSTER c WHERE mutation_id = 'm' TEST
KILL TRANSACTION WHERE tid = (1, 2, 3)
CHECK TABLE t PARTITION 1 SETTINGS check_query_single_value_result = 0
CHECK ALL TABLES
DELETE FROM t ON CLUSTER c WHERE id = 1
UPDATE t SET a = 1, b = b + 1 WHERE c = 2
SHOW TABLES FROM db LIKE 't%' LIMIT 10
SHOW TABLES NOT ILIKE '%tmp%' FORMAT JSON
SHOW DATABASES
SHOW COLUMNS FROM t FROM db LIKE 'a%'
SHOW INDEXES FROM t
SHOW SETTINGS LIKE 'max%'
SHOW CLUSTER 'c'
EXPLAIN AST SELECT 1
EXPLAIN SYNTAX oneline = 1 SELECT a FROM t WHERE a = 1
EXPLAIN PLAN actions = 1, indexes = 1 SELECT count() FROM t GROUP BY a
EXPLAIN PIPELINE graph = 1 SELECT 1
EXPLAIN QUERY TREE run_passes = 1 SELECT 1
EXPLAIN ESTIMATE SELECT * FROM t
EXPLAIN TABLE OVERRIDE mysql('host', 'db', 't', 'u', 'p') PARTITION BY toYYYYMM(d)
EXPLAIN CURRENT TRANSACTION
SYSTEM RELOAD DICTIONARY d
SYSTEM DROP MARK CACHE
SYSTEM DROP DATABASE REPLICA 'r' FROM DATABASE db WITH TABLES
SYSTEM SYNC REPLICA db.t STRICT
SYSTEM STOP MERGES ON VOLUME policy.volume
SYSTEM START MERGES t
SYSTEM FLUSH LOGS query_log, text_log
SYSTEM RESTART REPLICA t
SYSTEM DROP FILESYSTEM CACHE 'cache' KEY key OFFSET 0
SYSTEM ENABLE FAILPOINT fp
SYSTEM REFRESH VIEW mv
SYSTEM STOP LISTEN QUERIES ALL EXCEPT TCP, HTTP
SYSTEM UNFREEZE WITH NAME 'backup'
SYSTEM SHUTDOWN
BACKUP TABLE t, TEMPORARY TABLE tt, DATABASE db EXCEPT TABLES x, y TO Disk('backups', 'b.zip') SETTINGS compression_method = 'lzma'
BACKUP ALL EXCEPT DATABASES system, information_schema TO S3('https://bucket.s3.amazonaws.com/b', 'key', 'secret') ASYNC
RESTORE TABLE t AS t2 PARTITIONS 1, 2 FROM Disk('backups', 'b.zip') SETTINGS base_backup = Disk('backups', 'base.zip')
RESTORE DATABASE db AS db2 FROM File('b')
BEGIN TRANSACTION
COMMIT
ROLLBACK
SET TRANSACTION SNAPSHOT 42
