#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-encrypted-storage, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# test1: insert with transaction and mutation without transaction
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple();
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 4647429777703185695, 69, 4) LIMIT 86;

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 18218864097310396034, 228, 4) LIMIT 332;
    COMMIT;

    DELETE FROM t0 WHERE TRUE;
    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS REPLICATED;

    SELECT COUNT(*) FROM t0;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS NOT REPLICATED;

    DROP TABLE t0;
"

# test2: background merge with transaction and mutation without transaction
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple();
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 4647429777703185695, 69, 4) LIMIT 86;

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 18218864097310396034, 228, 4) LIMIT 332;
    ROLLBACK;

    INSERT INTO TABLE t0 (c0) SELECT CAST(number % 81 AS Int) FROM numbers(350);
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 14142794021619833828, 193, 4) LIMIT 164;
    INSERT INTO TABLE t0 (c0) SELECT 1 FROM numbers(322);
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 12294830401837572975, 254, 4) LIMIT 251;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 16932182231128798796, 132, 3) LIMIT 98;
    DELETE FROM t0 WHERE TRUE;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS REPLICATED;

    SELECT COUNT(*) FROM t0;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS NOT REPLICATED;

    DROP TABLE t0;
"

# test3: mutation withtransaction after ATTACH TO NOT REPLICATED
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY tuple();
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 4647429777703185695, 69, 4) LIMIT 86;

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) SELECT c0 FROM generateRandom('c0 Int', 18218864097310396034, 228, 4) LIMIT 332;
    COMMIT;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS REPLICATED;

    SELECT COUNT(*) FROM t0;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS NOT REPLICATED;

    BEGIN TRANSACTION;
    ALTER TABLE t0 UPDATE c0 = c0 + 1 WHERE 1;
    COMMIT;

    DROP TABLE t0;
"
