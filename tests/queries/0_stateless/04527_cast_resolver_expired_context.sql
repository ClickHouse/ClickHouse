SET enable_analyzer = 1;

SELECT '-- MATERIALIZED default evaluated on the insert pipeline';
DROP TABLE IF EXISTS t_cast_expired_insert;
CREATE TABLE t_cast_expired_insert (s String, ip IPv6 MATERIALIZED toIPv6OrDefault(s)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cast_expired_insert (s) VALUES ('::1'), ('not an ip');
SELECT s, ip FROM t_cast_expired_insert ORDER BY s;

SELECT '-- MATERIALIZED default whose cast overflows';
DROP TABLE IF EXISTS t_cast_expired_overflow;
-- Spell the conversion as an explicit `CAST` rather than `toDateTimeOrDefault`. Whether a
-- `to<Type>OrDefault` function reads `date_time_overflow_behavior` from the query context at all
-- depends on #109946, which is currently reverted on master and re-landed by #114912, so an
-- `OrDefault` spelling would make the expected value of this arm swing with that revert.
CREATE TABLE t_cast_expired_overflow (d Date, dt DateTime('UTC') MATERIALIZED CAST(d AS DateTime('UTC'))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cast_expired_overflow (d) VALUES ('2149-06-07'), ('2020-01-01');
SELECT d, dt FROM t_cast_expired_overflow ORDER BY d;

SELECT '-- MATERIALIZE COLUMN rebuilds the default on a mutation thread';
DROP TABLE IF EXISTS t_cast_expired_materialize;
CREATE TABLE t_cast_expired_materialize (a String, m Int64 MATERIALIZED toInt64OrDefault(a)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cast_expired_materialize (a) SELECT toString(number) FROM numbers(16);
-- Metadata-only: the stored bytes still hold the old expression's values.
ALTER TABLE t_cast_expired_materialize MODIFY COLUMN m Int64 MATERIALIZED toInt64OrDefault(a) + 1000;
SELECT count(), sum(m) FROM t_cast_expired_materialize;
ALTER TABLE t_cast_expired_materialize MATERIALIZE COLUMN m SETTINGS mutations_sync = 2;
SELECT count(), sum(m) FROM t_cast_expired_materialize;

SELECT '-- mutation expression containing casts';
DROP TABLE IF EXISTS t_cast_expired_update;
CREATE TABLE t_cast_expired_update (a String, b Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cast_expired_update SELECT toString(number), -1 FROM numbers(16);
SELECT count(), sum(b) FROM t_cast_expired_update;
ALTER TABLE t_cast_expired_update UPDATE b = toInt64OrDefault(a) WHERE toInt8OrDefault(a) >= 0 SETTINGS mutations_sync = 2;
SELECT count(), sum(b) FROM t_cast_expired_update;

SELECT '-- the settings the resolver snapshots stay in effect';
SELECT CAST(toDate('2149-06-07') AS DateTime('UTC')) SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(toDate('2149-06-07') AS DateTime('UTC')) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(toDate('2149-06-07') AS DateTime('UTC')) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT _CAST(toDate('2149-06-07'), 'DateTime(\'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT toTypeName(CAST(toNullable(toInt8(1)) AS Int32)) SETTINGS cast_keep_nullable = 1;
SELECT toTypeName(CAST(toNullable(toInt8(1)) AS Int32)) SETTINGS cast_keep_nullable = 0;
SELECT CAST('not an ip' AS IPv6) SETTINGS cast_ipv4_ipv6_default_on_conversion_error = 1;
SELECT toTypeName(CAST(toDateTime('2020-01-01 00:00:00', 'Europe/Amsterdam') AS DateTime));

SELECT '-- argument wrappers and boundaries';
SELECT toInt64OrDefault(toNullable('x'), 7::Int64), toInt64OrDefault(toLowCardinality('x'), 7::Int64);
SELECT accurateCastOrDefault(materialize('x'), 'Int64'), accurateCastOrDefault(CAST(NULL, 'Nullable(String)'), 'Int64');
SELECT toTypeName(accurateCastOrNull(toLowCardinality(toNullable('42')), 'Int64'));
SELECT toInt8OrDefault('', 1::Int8), toInt8OrDefault('999', 1::Int8), toInt64OrDefault('9223372036854775808', 1::Int64);
SELECT accurateCastOrNull('', 'Int8'), accurateCastOrNull('-128', 'Int8'), accurateCastOrNull('128', 'Int8');

DROP TABLE t_cast_expired_insert;
DROP TABLE t_cast_expired_overflow;
DROP TABLE t_cast_expired_materialize;
DROP TABLE t_cast_expired_update;
