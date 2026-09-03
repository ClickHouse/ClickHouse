#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user03562_${CLICKHOUSE_DATABASE}_$RANDOM";

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user";
${CLICKHOUSE_CLIENT} --query "CREATE USER $user";
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TEMPORARY TABLE ON *.* TO $user";

(( $(${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM gcs('http://localhost:8123/123/4', NOSIGN);" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3 TO $user";

(( $(${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM gcs('http://localhost:8123/123/4', NOSIGN);" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "UNEXPECTED" || echo "OK"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user";
