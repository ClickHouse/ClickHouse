#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user03636_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
-- Cleanup
DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF

${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3('http://localhost:11111/test/.*') TO $user WITH GRANT OPTION";

${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM s3('http://localhost:11111/test/a.tsv', 'TSV') FORMAT Null;";
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM s3('http://localhost:11111/a.tsv', 'TSV') FORMAT Null; -- { serverError ACCESS_DENIED }";
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM s3('http://localhost:11111/test/../a.tsv', 'TSV') FORMAT Null; -- { serverError ACCESS_DENIED }";
${CLICKHOUSE_CLIENT} --user $user --query "SELECT * FROM s3('http://localhost:11111/test/%2e%2e/a.tsv', 'TSV') FORMAT Null; -- { serverError ACCESS_DENIED }";

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
EOF
