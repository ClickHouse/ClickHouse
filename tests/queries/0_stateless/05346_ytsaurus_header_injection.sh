#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The OAuth token is sent in the Authorization header, so it must not contain CR or LF.
# The proxy URL points to a closed port, so an unchecked token fails with a network error instead.
# A full-definition ATTACH is user input like CREATE; Atomic and Replicated databases need a UUID for it.
UUID_CLAUSE=""
if [ "$($CLICKHOUSE_CLIENT -q "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")'"
fi

$CLICKHOUSE_CLIENT -m <<EOS
SET allow_experimental_ytsaurus_table_function = 1, allow_experimental_ytsaurus_table_engine = 1;

SELECT * FROM ytsaurus('http://127.0.0.1:1', '//tmp/t', 'token\r\nX-Injected: 1', 'x Int64'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM ytsaurus('http://127.0.0.1:1', '//tmp/t', 'token\nX-Injected: 1', 'x Int64'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_05346;
CREATE TABLE t_05346 (x Int64) ENGINE = YTsaurus('http://127.0.0.1:1', '//tmp/t', 'token\r\nX-Injected: 1'); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t_05346 ${UUID_CLAUSE} (x Int64) ENGINE = YTsaurus('http://127.0.0.1:1', '//tmp/t', 'token\r\nX-Injected: 1'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_05346';
DROP TABLE IF EXISTS t_05346;

SELECT 'OK';
EOS
