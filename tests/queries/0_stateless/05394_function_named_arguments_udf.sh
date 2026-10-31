#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL user-defined functions do not support named arguments. The function is global, so its name is unique per test database.
$CLICKHOUSE_CLIENT -q "
  CREATE FUNCTION ${CLICKHOUSE_DATABASE}_f AS (a, b) -> concat(b, a);
  SELECT ${CLICKHOUSE_DATABASE}_f(a => 'x', b => 'y'); -- { serverError BAD_ARGUMENTS }
  SELECT ${CLICKHOUSE_DATABASE}_f('x', 'y');
  DROP FUNCTION ${CLICKHOUSE_DATABASE}_f;
"
