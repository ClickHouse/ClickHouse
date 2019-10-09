#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


R1=table_1017_1
R2=table_1017_2
T1=table_1017_merge

${CLICKHOUSE_CLIENT} -n -q "
    DROP TABLE IF EXISTS $R1;
    DROP TABLE IF EXISTS $R2;

    CREATE TABLE $R1 (x UInt32, y UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_DATABASE}.table_1017', 'r1') ORDER BY x;
    CREATE TABLE $R2 (x UInt32, y UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_DATABASE}.table_1017', 'r2') ORDER BY x;
    CREATE TABLE $T1 (x UInt32, y UInt32) ENGINE MergeTree() ORDER BY x;

    INSERT INTO $R1 VALUES (0, 1)(1, 2)(2, 3)(3, 4);
    INSERT INTO $T1 VALUES (0, 1)(1, 2)(2, 3)(3, 4);
"

# Check that in mutations of replicated tables predicates do not contain non-deterministic functions
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $R1 DELETE WHERE ignore(rand())" 2>&1 \
| fgrep -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $R1 UPDATE y = y + rand() % 1 WHERE not ignore()" 2>&1 \
| fgrep -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'


# For regular tables we do not enforce deterministic functions
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $T1 DELETE WHERE rand() = 0" 2>&1 > /dev/null \
&& echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER TABLE $T1 UPDATE y = y + rand() % 1 WHERE not ignore()" 2>&1 > /dev/null \
&& echo 'OK' || echo 'FAIL'


${CLICKHOUSE_CLIENT} -n -q "
    DROP TABLE IF EXISTS $R2;
    DROP TABLE IF EXISTS $R1;
    DROP TABLE IF EXISTS $T1;
"
