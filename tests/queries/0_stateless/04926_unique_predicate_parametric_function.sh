#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS unique_predicate_parametric_function"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW unique_predicate_parametric_function AS SELECT topK(UNIQUE((SELECT number FROM numbers(3))))(number) FROM numbers(10)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM unique_predicate_parametric_function FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP VIEW unique_predicate_parametric_function"
