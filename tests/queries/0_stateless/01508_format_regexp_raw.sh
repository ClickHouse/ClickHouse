#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -n --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (a String, b String) ENGINE = Memory;
"

${CLICKHOUSE_CLIENT} --format_regexp_escaping_rule 'Raw' --format_regexp '^(.+?) separator (.+?)$' --query '
INSERT INTO t FORMAT Regexp abc\ separator Hello, world!'

${CLICKHOUSE_CLIENT} -n --query "
SELECT * FROM t;
DROP TABLE t;
"
