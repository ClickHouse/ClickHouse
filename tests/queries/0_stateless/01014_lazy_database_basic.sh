#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS testlazy"

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE testlazy ENGINE = Lazy(1);
    CREATE TABLE testlazy.log (a UInt64, b UInt64) ENGINE = Log;
    CREATE TABLE testlazy.slog (a UInt64, b UInt64) ENGINE = StripeLog;
    CREATE TABLE testlazy.tlog (a UInt64, b UInt64) ENGINE = TinyLog;
"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.parts WHERE database = 'testlazy'";

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT database, name, create_table_query FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT database, name FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM testlazy.log LIMIT 0; -- drop testlazy.log from cache
    RENAME TABLE testlazy.log TO testlazy.log2;
    SELECT database, name FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT database, name FROM system.tables WHERE database = 'testlazy';
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO testlazy.log2 VALUES (1, 1);
    INSERT INTO testlazy.slog VALUES (2, 2);
    INSERT INTO testlazy.tlog VALUES (3, 3);
    SELECT * FROM testlazy.log2;
    SELECT * FROM testlazy.slog;
    SELECT * FROM testlazy.tlog;
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM testlazy.log2 LIMIT 0; -- drop testlazy.log2 from cache
    DROP TABLE testlazy.log2;
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM testlazy.slog;
    SELECT * FROM testlazy.tlog;
"

sleep 1.5

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE testlazy;
"
