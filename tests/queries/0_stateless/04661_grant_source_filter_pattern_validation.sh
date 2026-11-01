#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The pattern of `GRANT READ ON S3('...')` is not compiled by the parser - that would put a regex
# engine in it - so every path that turns the query into access rights validates it instead. A
# pattern that does not compile is matched with `RE2::FullMatch`, so it would grant nothing while
# looking accepted.

user="user_04661_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user"

${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3('[') TO $user" 2>&1 | grep -q "CANNOT_COMPILE_REGEXP" && echo "OK"

# `CHECK GRANT` builds the same elements through its own interpreter.
${CLICKHOUSE_CLIENT} --query "CHECK GRANT READ ON S3('[')" 2>&1 | grep -q "CANNOT_COMPILE_REGEXP" && echo "OK"

# A valid pattern is accepted on both paths.
${CLICKHOUSE_CLIENT} --query "GRANT READ ON S3('s3://bucket/.*') TO $user"
${CLICKHOUSE_CLIENT} --query "CHECK GRANT READ ON S3('s3://bucket/.*')"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
