#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_function AS (x) -> x > 5;
  CREATE TABLE t0 (c0 Int, CONSTRAINT c1 CHECK ${CLICKHOUSE_DATABASE}_function(c0)) ENGINE = MergeTree() ORDER BY tuple();
  SHOW CREATE TABLE t0;
  INSERT INTO t0(c0) VALUES (10);
  INSERT INTO t0(c0) VALUES (3); -- {serverError VIOLATED_CONSTRAINT}
  SELECT * FROM t0;

  DROP TABLE t0;
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_function;
"
