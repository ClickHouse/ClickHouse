-- Verify that ALTER DELETE/UPDATE with DateTime comparisons works correctly
-- when session_timezone differs from server timezone. String literals in the
-- mutation predicate must be interpreted in the session timezone, not the
-- server default. The fix wraps them with toDateTime('...', '<tz>') at ALTER
-- time so the background mutation thread evaluates them consistently.

SET session_timezone = 'America/Denver'; -- UTC-7 (far from typical UTC server default)
SET mutations_sync = 2;

-- DateTime (no explicit timezone)
DROP TABLE IF EXISTS test_mutation_tz SYNC;
CREATE TABLE test_mutation_tz (id UInt32, time DateTime) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
SELECT 'before delete', id, time FROM test_mutation_tz ORDER BY id;

ALTER TABLE test_mutation_tz DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete', id, time FROM test_mutation_tz ORDER BY id;

-- DateTime64 (no explicit timezone)
DROP TABLE IF EXISTS test_mutation_tz64 SYNC;
CREATE TABLE test_mutation_tz64 (id UInt32, time DateTime64(3)) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz64 VALUES (1, '2000-01-01 01:02:03.123'), (2, '2000-01-01 04:05:06.456');
SELECT 'before delete dt64', id, time FROM test_mutation_tz64 ORDER BY id;

ALTER TABLE test_mutation_tz64 DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete dt64', id, time FROM test_mutation_tz64 ORDER BY id;

-- ALTER UPDATE with DateTime comparison in WHERE
DROP TABLE IF EXISTS test_mutation_tz_upd SYNC;
CREATE TABLE test_mutation_tz_upd (id UInt32, time DateTime, val String) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_upd VALUES (1, '2000-01-01 01:02:03', 'old'), (2, '2000-01-01 04:05:06', 'old');
ALTER TABLE test_mutation_tz_upd UPDATE val = 'new' WHERE time >= '2000-01-01 02:00:00';
SELECT 'after update', id, val FROM test_mutation_tz_upd ORDER BY id;

-- Nullable(DateTime) — the wrapper must be unwrapped before the timezone check
DROP TABLE IF EXISTS test_mutation_tz_nullable SYNC;
CREATE TABLE test_mutation_tz_nullable (id UInt32, time Nullable(DateTime)) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_nullable VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
SELECT 'before delete nullable', id, time FROM test_mutation_tz_nullable ORDER BY id;

ALTER TABLE test_mutation_tz_nullable DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete nullable', id, time FROM test_mutation_tz_nullable ORDER BY id;

-- LowCardinality(DateTime) — same unwrapping logic
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS test_mutation_tz_lc SYNC;
CREATE TABLE test_mutation_tz_lc (id UInt32, time LowCardinality(DateTime)) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_lc VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
SELECT 'before delete lc', id, time FROM test_mutation_tz_lc ORDER BY id;

ALTER TABLE test_mutation_tz_lc DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete lc', id, time FROM test_mutation_tz_lc ORDER BY id;

-- Nullable(DateTime64) — unwrapping for DateTime64
DROP TABLE IF EXISTS test_mutation_tz64_nullable SYNC;
CREATE TABLE test_mutation_tz64_nullable (id UInt32, time Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz64_nullable VALUES (1, '2000-01-01 01:02:03.123'), (2, '2000-01-01 04:05:06.456');
SELECT 'before delete nullable dt64', id, time FROM test_mutation_tz64_nullable ORDER BY id;

ALTER TABLE test_mutation_tz64_nullable DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'after delete nullable dt64', id, time FROM test_mutation_tz64_nullable ORDER BY id;

-- DateTime with explicit timezone should NOT be rewritten (timezone is already determined)
DROP TABLE IF EXISTS test_mutation_tz_explicit SYNC;
CREATE TABLE test_mutation_tz_explicit (id UInt32, time DateTime('UTC')) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_mutation_tz_explicit VALUES (1, '2000-01-01 01:02:03'), (2, '2000-01-01 04:05:06');
ALTER TABLE test_mutation_tz_explicit DELETE WHERE time >= '2000-01-01 02:00:00';
SELECT 'explicit tz', id, time FROM test_mutation_tz_explicit ORDER BY id;

DROP TABLE test_mutation_tz SYNC;
DROP TABLE test_mutation_tz64 SYNC;
DROP TABLE test_mutation_tz_upd SYNC;
DROP TABLE test_mutation_tz_nullable SYNC;
DROP TABLE test_mutation_tz_lc SYNC;
DROP TABLE test_mutation_tz64_nullable SYNC;
DROP TABLE test_mutation_tz_explicit SYNC;
