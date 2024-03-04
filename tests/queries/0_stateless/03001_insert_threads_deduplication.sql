DROP TABLE IF EXISTS landing SYNC;
DROP TABLE IF EXISTS ds SYNC;

CREATE TABLE landing
(
    timestamp DateTime64(3),
    status String,
    id String
)
ENGINE = MergeTree()
ORDER BY timestamp;

SYSTEM STOP MERGES landing; -- Stopping merges to force 3 parts

INSERT INTO landing (status, id, timestamp) SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO landing (status, id, timestamp) SELECT * FROM generateRandom() LIMIT 1;
INSERT INTO landing (status, id, timestamp) SELECT * FROM generateRandom() LIMIT 1;

CREATE TABLE ds
(
    timestamp DateTime64(3),
    status String,
    id String
)
ENGINE = MergeTree()
ORDER BY timestamp
SETTINGS non_replicated_deduplication_window=1000;

INSERT INTO ds SELECT * FROM landing
SETTINGS insert_deduplicate=1, insert_deduplication_token='token1',
         max_insert_threads=5;

SELECT count() FROM ds;

INSERT INTO ds SELECT * FROM landing
SETTINGS insert_deduplicate=1, insert_deduplication_token='token2',
         max_insert_threads=1;

SELECT count() FROM ds;

DROP TABLE IF EXISTS landing SYNC;
DROP TABLE IF EXISTS ds SYNC;
