#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

#test insert & delete without transaction
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE mt (n int) ENGINE = MergeTree() ORDER BY tuple();

    INSERT INTO mt VALUES (1);
    INSERT INTO mt VALUES (2);

    ALTER TABLE mt DELETE WHERE n = 1;

    DETACH TABLE mt;
    ATTACH TABLE mt AS REPLICATED;
    DROP TABLE mt;
"

#test insert with transation rollback
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE mt (n int) ENGINE = MergeTree() ORDER BY tuple();

    BEGIN TRANSACTION;
    INSERT INTO mt VALUES (1);
    ROLLBACK;

    INSERT INTO mt VALUES (2);

    DETACH TABLE mt;
"

${CLICKHOUSE_CLIENT} --query="ATTACH TABLE mt AS REPLICATED" 2>&1 | grep -Eo "CANNOT_RESTORE_TABLE" | uniq

${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE mt;
    DROP TABLE mt;"

#test insert with transation commit
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE mt (n int) ENGINE = MergeTree() ORDER BY tuple();

    BEGIN TRANSACTION;
    INSERT INTO mt VALUES (1);
    COMMIT;

    INSERT INTO mt VALUES (2);

    DETACH TABLE mt;
"

${CLICKHOUSE_CLIENT} --query="ATTACH TABLE mt AS REPLICATED" 2>&1 | grep -Eo "CANNOT_RESTORE_TABLE" | uniq

${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE mt;
    DROP TABLE mt;"

#test delete with transation rollback
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE mt (n int) ENGINE = MergeTree() ORDER BY tuple();

    INSERT INTO mt VALUES (1);
    INSERT INTO mt VALUES (2);

    BEGIN TRANSACTION;
    ALTER TABLE mt DELETE WHERE n = 1;
    ROLLBACK;


    DETACH TABLE mt;
"

${CLICKHOUSE_CLIENT} --query="ATTACH TABLE mt AS REPLICATED" 2>&1 | grep -Eo "CANNOT_RESTORE_TABLE" | uniq

${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE mt;
    DROP TABLE mt;"

#test delete with transation rollback
${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE mt (n int) ENGINE = MergeTree() ORDER BY tuple();

    INSERT INTO mt VALUES (1);
    INSERT INTO mt VALUES (2);

    BEGIN TRANSACTION;
    ALTER TABLE mt DELETE WHERE n = 1;
    COMMIT;


    DETACH TABLE mt;
"

${CLICKHOUSE_CLIENT} --query="ATTACH TABLE mt AS REPLICATED" 2>&1 | grep -Eo "CANNOT_RESTORE_TABLE" | uniq

${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE mt;
    DROP TABLE mt; "
