#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL='fatal'

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT "
DROP TABLE IF EXISTS cheque;
CREATE TABLE cheque (s String) ORDER BY s;
INSERT INTO cheque VALUES ('Check how the cheque checks out on the checkerboard.');

CHECK TABLE cheque;
SELECT * FROM cheque;

CREATE USER test_03312;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.cheque TO test_03312;
"

$CLICKHOUSE_CLIENT --user test_03312 "
SELECT * FROM cheque;
"

$CLICKHOUSE_CLIENT --user test_03312 "
CHECK TABLE cheque;
" 2>&1 | grep -o -F 'ACCESS_DENIED'

$CLICKHOUSE_CLIENT "
GRANT CHECK ON ${CLICKHOUSE_DATABASE}.cheque TO test_03312;
"

$CLICKHOUSE_CLIENT --user test_03312 "
CHECK TABLE cheque;
"

$CLICKHOUSE_CLIENT "
DROP TABLE cheque;
DROP USER test_03312;
"
