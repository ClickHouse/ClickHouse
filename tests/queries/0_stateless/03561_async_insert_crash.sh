#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_03561_async_insert.data"
trap 'rm -f $FILE' EXIT

$CLICKHOUSE_CLIENT -q "
  DROP TABLE IF EXISTS t0;
  CREATE TABLE t0 (c0 Int) ENGINE = Null();
  INSERT INTO TABLE FUNCTION file('${FILE}', 'JSONColumns', 'c0 Int') SELECT 1 FROM t0 LIMIT 0;
  SET async_insert = 1;
  INSERT INTO TABLE t0 (c0) FROM INFILE '${FILE}' FORMAT JSONColumns;
"