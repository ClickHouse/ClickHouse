-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- no-fasttest: rocksdb is not enabled in fasttest.

-- A key lookup narrows the candidates, so a literal of another type from the date family has to be
-- converted with its own type: reinterpreting its raw value in the key's unit space (days against
-- seconds) probes a key that does not exist, and the query silently returns no rows.

DROP TABLE IF EXISTS t_kv_date_literal;

CREATE TABLE t_kv_date_literal (key Date, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_date_literal VALUES ('2024-01-02', 'a');

SELECT count() FROM t_kv_date_literal WHERE key = toDate('2024-01-02');
SELECT count() FROM t_kv_date_literal WHERE key = toDateTime('2024-01-02 00:00:00', 'UTC');
SELECT count() FROM t_kv_date_literal WHERE key = toDateTime64('2024-01-02 00:00:00', 3, 'UTC');
SELECT count() FROM t_kv_date_literal WHERE key IN (toDateTime('2024-01-02 00:00:00', 'UTC'));
SELECT count() FROM t_kv_date_literal WHERE key = toDateTime('2024-01-03 00:00:00', 'UTC');

DROP TABLE t_kv_date_literal;

CREATE TABLE t_kv_date_literal (key DateTime('UTC'), value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_date_literal VALUES ('2024-01-02 00:00:00', 'a');

SELECT count() FROM t_kv_date_literal WHERE key = toDateTime('2024-01-02 00:00:00', 'UTC');
SELECT count() FROM t_kv_date_literal WHERE key = toDate('2024-01-02');
SELECT count() FROM t_kv_date_literal WHERE key = toDateTime64('2024-01-02 00:00:00', 3, 'UTC');
SELECT count() FROM t_kv_date_literal WHERE key = toDate('2024-01-03');

DROP TABLE t_kv_date_literal;

CREATE TABLE t_kv_date_literal (key DateTime64(3, 'UTC'), value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_date_literal VALUES ('2024-01-02 00:00:00.000', 'a');

SELECT count() FROM t_kv_date_literal WHERE key = toDateTime64('2024-01-02 00:00:00', 3, 'UTC');
SELECT count() FROM t_kv_date_literal WHERE key = toDate('2024-01-02');
SELECT count() FROM t_kv_date_literal WHERE key = toDateTime('2024-01-02 00:00:00', 'UTC');

DROP TABLE t_kv_date_literal;

-- A wrapped literal carries the same type: the wrappers must not hide the date-family conversion.

CREATE TABLE t_kv_date_literal (key DateTime('UTC'), value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO t_kv_date_literal VALUES ('2024-01-02 00:00:00', 'a');

SELECT count() FROM t_kv_date_literal WHERE key = CAST(toDate('2024-01-02'), 'Nullable(Date)');
SELECT count() FROM t_kv_date_literal WHERE key = CAST(toDate('2024-01-02'), 'LowCardinality(Date)') SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT count() FROM t_kv_date_literal WHERE key IN (CAST(toDate('2024-01-02'), 'Nullable(Date)'));

DROP TABLE t_kv_date_literal;

-- The same for a composite key, through equality and through `IN`.

CREATE TABLE t_kv_date_literal (dt Date, id UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY (dt, id);
INSERT INTO t_kv_date_literal VALUES ('2024-01-02', 1, 'a');

SELECT count() FROM t_kv_date_literal WHERE (dt, id) = (toDateTime('2024-01-02 00:00:00', 'UTC'), 1);
SELECT count() FROM t_kv_date_literal WHERE (dt, id) = (CAST(toDate('2024-01-02'), 'Nullable(Date)'), 1);
SELECT count() FROM t_kv_date_literal WHERE (dt, id) IN ((toDateTime('2024-01-02 00:00:00', 'UTC'), 1));
-- A `DateTime64` element of a tuple set is refused while the set itself is built, before any key
-- filter sees it - the same on `MergeTree`.
SELECT count() FROM t_kv_date_literal WHERE (dt, id) IN ((toDateTime64('2024-01-02 00:00:00', 3, 'UTC'), 1)); -- { serverError TYPE_MISMATCH }
SELECT count() FROM t_kv_date_literal WHERE (dt, id) = (toDate('2024-01-03'), 1);

DROP TABLE t_kv_date_literal;
