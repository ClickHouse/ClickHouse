#!/usr/bin/env bash

set -o errexit
set -o pipefail

echo "
    DROP TABLE IF EXISTS test.two_blocks;
    CREATE TABLE test.two_blocks (d Date) ENGINE = MergeTree(d, d, 1);
    INSERT INTO test.two_blocks VALUES ('2000-01-01');
    INSERT INTO test.two_blocks VALUES ('2000-01-02');
" | clickhouse-client -n

for i in {1..10}; do seq 1 100 | sed 's/.*/SELECT count() FROM (SELECT * FROM test.two_blocks);/' | clickhouse-client -n --receive_timeout=1 | grep -vE '^2$' && echo 'Fail!' && break; echo -n '.'; done; echo

echo "DROP TABLE test.two_blocks;" | clickhouse-client -n
