SET allow_experimental_inverted_index=1;

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    `t` UInt64,
    `s` String,
    INDEX idx lower(s) TYPE inverted(3, 8192) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY t
PARTITION BY t;

INSERT INTO t VALUES (1, 'The sun was setting over the horizon, painting the sky orange.');
INSERT INTO t VALUES (2, 'She sipped her coffee, enjoying the warmth spreading through her body.');
INSERT INTO t VALUES (3, 'The cat lazily stretched out on the windowsill, basking in the sun.');
INSERT INTO t VALUES (4, 'The sound of the waves crashing against the shore was soothing.');
INSERT INTO t VALUES (5, 'He could not resist the temptation of the freshly baked cookies.');
INSERT INTO t VALUES (6, 'The leaves rustled in the wind, creating a peaceful melody.');
INSERT INTO t VALUES (7, 'The city was bustling with people, each with their own story.');
INSERT INTO t VALUES (8, 'The smell of the flowers filled the air, intoxicating and sweet.');
INSERT INTO t VALUES (9, 'The stars twinkled in the night sky, a reminder of the vast universe.');
INSERT INTO t VALUES (10, 'The book was captivating, transporting her to another world.');


-- line 1
SELECT count() FROM t WHERE lower(s) LIKE '%the%';
SYSTEM FLUSH LOGS;

-- line 2
SELECT ProfileEvents['InvertedIndexMetadataCacheHits'], ProfileEvents['InvertedIndexMetadataCacheMisses']
FROM system.query_log
PREWHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '-- line 1%SELECT count() FROM t WHERE lower(s) LIKE %'
ORDER BY query_start_time DESC LIMIT 1
FORMAT CSV;


-- line 3
SELECT count() FROM t WHERE lower(s) LIKE '%the%';
SYSTEM FLUSH LOGS;

-- line 4
SELECT ProfileEvents['InvertedIndexMetadataCacheHits'], ProfileEvents['InvertedIndexMetadataCacheMisses']
FROM system.query_log
PREWHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '-- line 3%SELECT count() FROM t WHERE lower(s) LIKE %'
ORDER BY query_start_time DESC LIMIT 1
FORMAT CSV;

-- `SYSTEM DROP INVERTED INDEX CACHE` is server wide, don't test it with any concurrency.

-- Queries below are commented because they can become flaky with Ordinary databases, for the time being.

-- TRUNCATE TABLE t;
-- INSERT INTO t VALUES (1, 'The sun was setting over the horizon, painting the sky orange.');

-- -- line 5
-- SELECT count() FROM t WHERE lower(s) LIKE '%the%';
-- SYSTEM FLUSH LOGS;

-- -- line 6
-- SELECT ProfileEvents['InvertedIndexMetadataCacheHits'], ProfileEvents['InvertedIndexMetadataCacheMisses']
-- FROM system.query_log
-- PREWHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '-- line 5%SELECT count() FROM t WHERE lower(s) LIKE %'
-- ORDER BY query_start_time DESC LIMIT 1
-- FORMAT CSV;

DROP TABLE t SYNC;