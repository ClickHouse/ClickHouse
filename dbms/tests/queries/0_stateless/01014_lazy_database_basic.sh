#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CLIENT} -n -q "
    CREATE DATABASE testlazy ENGINE = Lazy(1);
    CREATE TABLE testlazy.log (a UInt64, b UInt64) ENGINE = Log;
    CREATE TABLE testlazy.slog (a UInt64, b UInt64) ENGINE = StripeLog;
    CREATE TABLE testlazy.tlog (a UInt64, b UInt64) ENGINE = TinyLog;
"

sleep 2

${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM system.tables WHERE database = 'testlazy';
"

sleep 2

${CLICKHOUSE_CLIENT} -q "
    SELECT database, name, metadata_modification_time FROM system.tables WHERE database = 'testlazy';
"

sleep 2

${CLICKHOUSE_CLIENT} -n -q "
    INSERT INTO testlazy.log VALUES (1, 1);
    INSERT INTO testlazy.slog VALUES (2, 2);
    INSERT INTO testlazy.tlog VALUES (3, 3);
    SELECT * FROM testlazy.log;
    SELECT * FROM testlazy.slog;
    SELECT * FROM testlazy.tlog;
"

sleep 2

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE testlazy.log;
"

sleep 2

${CLICKHOUSE_CLIENT} -n -q "
    SELECT * FROM testlazy.slog;
    SELECT * FROM testlazy.tlog;
"

sleep 2

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE testlazy;
"
