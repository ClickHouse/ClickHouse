#!/usr/bin/env bash
# Tags: no-fasttest, long, no-debug, no-object-storage
# This test is too slow with S3 storage and debug modes.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

it=0
TIMELIMIT=31
while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 100 ];
do
    it=$((it+1))
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS mt;
        CREATE TABLE mt (x UInt8, k UInt8 DEFAULT 0) ENGINE = SummingMergeTree ORDER BY k;

        INSERT INTO mt (x) VALUES (1);
        INSERT INTO mt (x) VALUES (2);
        INSERT INTO mt (x) VALUES (3);
        INSERT INTO mt (x) VALUES (4);
        INSERT INTO mt (x) VALUES (5);
        INSERT INTO mt (x) VALUES (6);
        INSERT INTO mt (x) VALUES (7);
        INSERT INTO mt (x) VALUES (8);
        INSERT INTO mt (x) VALUES (9);
        INSERT INTO mt (x) VALUES (10);

        OPTIMIZE TABLE mt FINAL;
    ";

    RES=$($CLICKHOUSE_CLIENT --query "SELECT * FROM mt;")
    if [ "$RES" !=  "55	0" ]; then
        echo "FAIL. Got: $RES"
    fi

    $CLICKHOUSE_CLIENT --query "DROP TABLE mt;"
done
