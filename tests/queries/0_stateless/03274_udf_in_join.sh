#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
  CREATE VIEW v0 AS SELECT 1 AS c0;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_second AS (x, y) -> y;
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_equals AS (x, y) -> x = y;
  SET optimize_rewrite_array_exists_to_has = 1;

  EXPLAIN PLAN SELECT 1 FROM v0 JOIN v0 vx ON ${CLICKHOUSE_DATABASE}_second(v0.c0, vx.c0); -- { serverError INVALID_JOIN_ON_EXPRESSION }
  EXPLAIN SYNTAX SELECT 1 FROM v0 JOIN v0 vx ON ${CLICKHOUSE_DATABASE}_equals(v0.c0, vx.c0);

  SELECT 1 FROM v0 JOIN v0 vx ON ${CLICKHOUSE_DATABASE}_equals(v0.c0, vx.c0);

  DROP view v0;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_second;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_equals;
"
