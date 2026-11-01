#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
  DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_f0;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_f0 AS (p0) -> p0;

  CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY ${CLICKHOUSE_DATABASE}_f0(${CLICKHOUSE_DATABASE}_f0(COLUMNS('1'))) SETTINGS log_formatted_queries = 1; -- { serverError BAD_ARGUMENTS };

  DROP FUNCTION ${CLICKHOUSE_DATABASE}_f0;
"