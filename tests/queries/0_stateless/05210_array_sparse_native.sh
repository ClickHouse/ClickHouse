#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

table="${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${table} (a Array(String), b Array(Nullable(String)))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS serialization_info_version = 'with_subcolumns',
        ratio_of_defaults_for_sparse_serialization = 0.5,
        nullable_serialization_version = 'allow_sparse';
    INSERT INTO ${table}
    SELECT [if(number = 0, 'value', '')], [if(number = 0, 'value', NULL)]
    FROM numbers(100);
    SELECT dumpColumnStructure(a) LIKE '%Sparse%', dumpColumnStructure(b) LIKE '%Sparse%'
    FROM ${table} LIMIT 1;
"

check="SELECT count(), countIf(a = ['value']), countIf(a = ['']),
    countIf(b = ['value']), countIf(isNull(b[1])) FROM table"

# Exercise both TCP and revision-zero HTTP `Native` output without a `JSON` descendant.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${table} FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query "${check}"
${CLICKHOUSE_CURL} -sS --fail "${CLICKHOUSE_URL}" --data-binary \
    "SELECT * FROM ${CLICKHOUSE_DATABASE}.${table} FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query "${check}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
