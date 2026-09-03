#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} "
DROP TABLE IF EXISTS test;
CREATE TABLE test (s String) ORDER BY ();
INSERT INTO test VALUES ('Hello, world!');
"

${CLICKHOUSE_LOCAL} "
CREATE NAMED COLLECTION mydb AS host = '${CLICKHOUSE_HOST}', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '', db = '${CLICKHOUSE_DATABASE}';
SELECT * FROM remote(mydb, table = 'test');
" 2>&1 | grep --text -F -v "ASan doesn't fully support makecontext/swapcontext functions"

${CLICKHOUSE_CLIENT} "
DROP TABLE test;
"
