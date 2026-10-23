-- A strict comparison of a sub-second DateTime64 key with an integer or DateTime constant must not skip rows.

DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_pk_nullable;
DROP TABLE IF EXISTS t_minmax;
DROP TABLE IF EXISTS t_part;

CREATE TABLE t_pk (dt DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_pk VALUES ('2020-01-01 00:00:00.000'), ('2020-01-01 00:00:00.300'), ('2020-01-01 00:00:00.500'), ('2020-01-01 00:00:01.000');

SELECT 'pk >', count() FROM t_pk WHERE dt > 1577836800;
SELECT 'pk <', count() FROM t_pk WHERE dt < 1577836801;
SELECT 'pk reversed', count() FROM t_pk WHERE 1577836800 < dt;
SELECT 'pk < DateTime', count() FROM t_pk WHERE dt < toDateTime('2020-01-01 00:00:01', 'UTC');
SELECT 'pk NOT <=', count() FROM t_pk WHERE NOT (dt <= 1577836800);
SELECT 'pk monotonic chain', count() FROM t_pk WHERE toDateTime64(dt, 6) > 1577836800;
SELECT 'pk pruning kept', count() FROM t_pk WHERE dt < 1577836000 SETTINGS force_primary_key = 1, max_rows_to_read = 1;

CREATE TABLE t_pk_nullable (dt Nullable(DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY dt SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_pk_nullable VALUES ('2020-01-01 00:00:00.000'), ('2020-01-01 00:00:00.300'), ('2020-01-01 00:00:00.500'), ('2020-01-01 00:00:01.000');

SELECT 'nullable pk >', count() FROM t_pk_nullable WHERE dt > 1577836800;

CREATE TABLE t_minmax (time DateTime64(9, 'UTC'), INDEX idx_time time TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_minmax VALUES (0::Decimal(9, 2)::DateTime64(9, 'UTC')), (0.01::Decimal(9, 2)::DateTime64(9, 'UTC')), (0.99::Decimal(9, 2)::DateTime64(9, 'UTC')), (1::Decimal(9, 2)::DateTime64(9, 'UTC')), (1.01::Decimal(9, 2)::DateTime64(9, 'UTC'));

SELECT 'minmax >', count() FROM t_minmax WHERE time > 0 SETTINGS force_data_skipping_indices = 'idx_time';
SELECT 'minmax <', count() FROM t_minmax WHERE time < 1 SETTINGS force_data_skipping_indices = 'idx_time';

CREATE TABLE t_part (dt DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY dt ORDER BY tuple();
INSERT INTO t_part VALUES ('2020-01-01 00:00:00.000'), ('2020-01-01 00:00:00.300'), ('2020-01-01 00:00:00.500'), ('2020-01-01 00:00:01.000');

SELECT 'partition >', count() FROM t_part WHERE dt > 1577836800;
SELECT 'partition <', count() FROM t_part WHERE dt < 1577836801;

DROP TABLE t_pk;
DROP TABLE t_pk_nullable;
DROP TABLE t_minmax;
DROP TABLE t_part;
