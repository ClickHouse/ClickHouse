#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Values that become HTTP header values must not contain CR or LF.
# The base_url points to a closed port, so an unchecked value fails with a network error instead.
# A full-definition ATTACH is user input like CREATE; Atomic and Replicated databases need a UUID for it.
UUID_CLAUSE=""
if [ "$($CLICKHOUSE_CLIENT -q "SELECT engine FROM system.databases WHERE name = currentDatabase()")" != "Ordinary" ]; then
    UUID_CLAUSE="UUID '$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")'"
fi

$CLICKHOUSE_CLIENT -m <<EOS
SELECT * FROM bigquery('project', 'dataset', 'table', 'token', billing_project = 'p\r\nX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM bigquery('project', 'dataset', 'table', 'token\nX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_05345;
CREATE TABLE t_05345 (x Int64) ENGINE = BigQuery('project', 'dataset', 'table', 'token', billing_project = 'p\r\nX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_05345 (x Int64) ENGINE = BigQuery('project', 'dataset', 'table', 'token\rX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t_05345 ${UUID_CLAUSE} (x Int64) ENGINE = BigQuery('project', 'dataset', 'table', 'token', billing_project = 'p\r\nX-Injected: 1', base_url = 'http://127.0.0.1:1'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_05345';
DROP TABLE IF EXISTS t_05345;

SELECT 'OK';
EOS
