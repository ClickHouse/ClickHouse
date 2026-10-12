-- With data_type_default_nullable = 1 a SimpleAggregateFunction column is not made Nullable(SimpleAggregateFunction(...)),
-- whose function merges do not apply: the declaration throws.

SET data_type_default_nullable = 1;

CREATE TABLE t_create (k UInt32 NOT NULL, m SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_default_null (k UInt32 NOT NULL, m SimpleAggregateFunction(sum, UInt64) DEFAULT NULL) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_typeless (k UInt32 NOT NULL, m DEFAULT CAST(0, 'SimpleAggregateFunction(sum, UInt64)')) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An explicit NOT NULL on a column without a type is honored too.
DROP TABLE IF EXISTS t_typeless_not_null;
CREATE TABLE t_typeless_not_null (k UInt32 NOT NULL, m NOT NULL DEFAULT CAST(0, 'SimpleAggregateFunction(sum, UInt64)'), u NOT NULL DEFAULT toUInt64(0)) ENGINE = MergeTree ORDER BY k;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_typeless_not_null' ORDER BY position;
DROP TABLE t_typeless_not_null;

DROP TABLE IF EXISTS t_saf;
CREATE TABLE t_saf (k UInt32 NOT NULL, m SimpleAggregateFunction(sum, UInt64) NOT NULL, n SimpleAggregateFunction(sum, Nullable(UInt64))) ENGINE = AggregatingMergeTree ORDER BY k;
ALTER TABLE t_saf ADD COLUMN a SimpleAggregateFunction(sum, UInt64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE t_saf MODIFY COLUMN m SimpleAggregateFunction(sum, UInt64) COMMENT 'm'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE t_saf ADD COLUMN a SimpleAggregateFunction(sum, UInt64) NOT NULL;
ALTER TABLE t_saf MODIFY COLUMN m SimpleAggregateFunction(sum, UInt64) NOT NULL COMMENT 'm';
INSERT INTO t_saf VALUES (1, 10, 10, 10);
INSERT INTO t_saf VALUES (1, 20, 20, 20);
OPTIMIZE TABLE t_saf FINAL;
SELECT * FROM t_saf;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_saf' ORDER BY position;
DROP TABLE t_saf;

-- The NULL modifier and an explicit Nullable(SimpleAggregateFunction(...)) are kept, and such a table loads.
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (k UInt32 NOT NULL, s SimpleAggregateFunction(sum, UInt64) NULL, e Nullable(SimpleAggregateFunction(sum, UInt64))) ENGINE = MergeTree ORDER BY k;
DETACH TABLE t_nullable;
ATTACH TABLE t_nullable;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_nullable' ORDER BY position;
DROP TABLE t_nullable;
