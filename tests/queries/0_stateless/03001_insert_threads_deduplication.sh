#!/bin/bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "
DROP TABLE IF EXISTS landing SYNC;
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

DROP TABLE IF EXISTS ds SYNC;

CREATE TABLE ds
(
    timestamp DateTime64(3),
    status String,
    id String
)
ENGINE = MergeTree()
ORDER BY timestamp
SETTINGS non_replicated_deduplication_window=1000;

SELECT 'This bug has been there forever. Present in 22.2';
SELECT '- When using multiple threads the insert produces 3 parts causing undesired deduplication.';
SELECT '- When using a single thread the insert produces 1 part without deduplication.';

INSERT INTO ds SELECT * FROM landing
SETTINGS insert_deduplicate=1, insert_deduplication_token='token1',
         max_insert_threads=5;

SELECT count() FROM ds;

INSERT INTO ds SELECT * FROM landing
SETTINGS insert_deduplicate=1, insert_deduplication_token='token2',
         max_insert_threads=1;

SELECT count() FROM ds;
" | $CLICKHOUSE_CLIENT -n

echo "
CREATE TABLE ds_remote
(
    timestamp DateTime64(3),
    status String,
    id String
)
ENGINE = MergeTree()
ORDER BY timestamp
SETTINGS non_replicated_deduplication_window=1000;

SELECT 'This bug has been introduced in CH 24.2+. See https://github.com/ClickHouse/ClickHouse/pull/59448';
SELECT '- When using remote function and multiple threads the insert produces 3 parts causing undesired deduplication.';
SELECT '- When using remote function and a single thread the insert produces 1 part without deduplication.';

INSERT INTO ds_remote SELECT * FROM remote('localhost:$CLICKHOUSE_PORT_TCP', $CLICKHOUSE_DATABASE, landing)
SETTINGS insert_deduplicate=1, insert_deduplication_token='token1',
         max_insert_threads=5;

SELECT count() FROM ds_remote;

INSERT INTO ds_remote SELECT * FROM remote('localhost:$CLICKHOUSE_PORT_TCP', $CLICKHOUSE_DATABASE, landing)
SETTINGS insert_deduplicate=1, insert_deduplication_token='token2',
         max_insert_threads=1;

SELECT count() FROM ds_remote;
" | $CLICKHOUSE_LOCAL -n

echo "
DROP TABLE IF EXISTS landing SYNC;
DROP TABLE IF EXISTS ds SYNC;
" | $CLICKHOUSE_CLIENT -n
