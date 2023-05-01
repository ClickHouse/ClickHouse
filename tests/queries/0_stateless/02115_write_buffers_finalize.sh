#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: depends on brotli and bzip2

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for m in gz br xz zst bz2 
do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS file"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE file (x UInt64) ENGINE = File(Native, '${CLICKHOUSE_DATABASE}/${m}.data.${m}')"
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE file"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO file SELECT * FROM numbers(100000)"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), max(x) FROM file"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE file"
done

