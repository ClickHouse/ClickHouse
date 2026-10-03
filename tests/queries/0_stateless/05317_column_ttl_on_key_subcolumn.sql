-- A column whose subcolumn is used in a table key is a key column: a column TTL on it is refused,
-- and neither a TTL merge nor an UPDATE may change it by recomputing a MATERIALIZED expression.

-- Column TTL on a tuple whose element is read by the primary key (unnamed tuple, backquoted element).
DROP TABLE IF EXISTS t_unnamed;
CREATE TABLE t_unnamed (c0 Date, c1 Tuple(UInt8) TTL c0 + INTERVAL 8 SECOND) ENGINE = MergeTree PRIMARY KEY cityHash64(`c1.1`); -- { serverError ILLEGAL_COLUMN }

-- Named tuple element in the sorting key.
DROP TABLE IF EXISTS t_named;
CREATE TABLE t_named (d Date, t Tuple(a UInt8) TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY t.a; -- { serverError ILLEGAL_COLUMN }

-- Named tuple element in the partition key.
DROP TABLE IF EXISTS t_partition;
CREATE TABLE t_partition (d Date, t Tuple(a UInt8) TTL d + INTERVAL 1 DAY) ENGINE = MergeTree PARTITION BY t.a ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

-- JSON path in the sorting key.
DROP TABLE IF EXISTS t_json;
CREATE TABLE t_json (d Date, json JSON TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY json.a::Int64; -- { serverError ILLEGAL_COLUMN }

-- A TTL on a column the key does not read is allowed.
DROP TABLE IF EXISTS t_alter;
CREATE TABLE t_alter (d Date, t Tuple(a UInt8), v UInt8 TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY t.a;

-- ALTER cannot add such a TTL either.
ALTER TABLE t_alter MODIFY COLUMN t Tuple(a UInt8) TTL d + INTERVAL 1 DAY; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t_alter ADD COLUMN c Tuple(a UInt8) TTL d + INTERVAL 1 DAY, MODIFY ORDER BY (t.a, c.a); -- { serverError ILLEGAL_COLUMN }
DROP TABLE t_alter;

-- A full-definition ATTACH states a new definition and is refused like CREATE.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (c0 Date, c1 Tuple(UInt8) TTL c0 + INTERVAL 8 SECOND) ENGINE = MergeTree PRIMARY KEY cityHash64(`c1.1`); -- { serverError ILLEGAL_COLUMN }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A MATERIALIZED column whose subcolumn is in the sorting key keeps its value when the TTL of its source
-- column expires, so the merged part stays sorted.
DROP TABLE IF EXISTS t_materialized_ttl;
CREATE TABLE t_materialized_ttl (d DateTime, x Int32 TTL d + INTERVAL 1 SECOND, m Tuple(a Int32) MATERIALIZED tuple(x + 1))
ENGINE = MergeTree ORDER BY m.a SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_materialized_ttl (d, x) VALUES ('2000-01-01 00:00:00', 100), ('2100-01-01 00:00:00', 7);
OPTIMIZE TABLE t_materialized_ttl FINAL;
SELECT x, m.a FROM t_materialized_ttl ORDER BY d;
INSERT INTO t_materialized_ttl (d, x) VALUES ('2100-01-01 00:00:00', 50);
OPTIMIZE TABLE t_materialized_ttl FINAL;
SELECT x, m.a FROM t_materialized_ttl ORDER BY m.a;
DROP TABLE t_materialized_ttl;

-- UPDATE of a column that a MATERIALIZED key column is computed from is refused.
DROP TABLE IF EXISTS t_materialized_update;
CREATE TABLE t_materialized_update (x Int32, m Tuple(a Int32) MATERIALIZED tuple(x + 1)) ENGINE = MergeTree ORDER BY m.a;
INSERT INTO t_materialized_update (x) VALUES (1), (100);
ALTER TABLE t_materialized_update UPDATE x = 200 - x WHERE 1 SETTINGS mutations_sync = 2; -- { serverError CANNOT_UPDATE_COLUMN }
SELECT x, m.a FROM t_materialized_update ORDER BY m.a;
DROP TABLE t_materialized_update;
