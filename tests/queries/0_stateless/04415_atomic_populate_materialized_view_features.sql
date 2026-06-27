-- Atomic POPULATE features: basic population, POPULATE with TO, Memory source, non-atomic fallback for a
-- source that cannot provide a pinned snapshot, and the opt-out setting.

-- 1) Atomic POPULATE from a MergeTree source backfills existing data, and the view then receives new rows.
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(10);
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src;
SELECT 'mergetree_populate', count(), sum(id) FROM mv;
INSERT INTO src VALUES (100);
SELECT 'mergetree_after_insert', count(), sum(id) FROM mv;
DROP TABLE mv;
DROP TABLE src;

-- 2) POPULATE together with TO is now allowed and backfills the (possibly non-empty) target table.
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS target;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(10);
CREATE TABLE target (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO target VALUES (1000);
CREATE MATERIALIZED VIEW mv TO target POPULATE AS SELECT id FROM src;
SELECT 'to_populate', count(), sum(id) FROM target;
INSERT INTO src VALUES (200);
SELECT 'to_after_insert', count(), sum(id) FROM target;
DROP TABLE mv;
DROP TABLE target;
DROP TABLE src;

-- 3) A Memory source is supported (it provides a pinned read-time snapshot).
DROP TABLE IF EXISTS src_mem;
DROP TABLE IF EXISTS mv;
CREATE TABLE src_mem (id UInt64) ENGINE = Memory;
INSERT INTO src_mem SELECT number FROM numbers(5);
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src_mem;
SELECT 'memory_populate', count() FROM mv;
DROP TABLE mv;
DROP TABLE src_mem;

-- 4) A source that cannot provide a pinned snapshot (Log family, views, Distributed, ...) is populated
-- non-atomically (the legacy behavior, with a warning in the log) instead of failing.
DROP TABLE IF EXISTS src_log;
DROP TABLE IF EXISTS mv;
CREATE TABLE src_log (id UInt64) ENGINE = Log;
INSERT INTO src_log SELECT number FROM numbers(5);
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src_log;
SELECT 'log_populate_fallback', count() FROM mv;
DROP TABLE mv;
DROP TABLE src_log;

-- 5) Disabling the setting uses the legacy non-atomic population for any source.
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(7);
SET materialized_views_populate_atomically = 0;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src;
SELECT 'optout_populate', count() FROM mv;
DROP TABLE mv;
DROP TABLE src;
