#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}
    (
        t Tuple(a UInt64),
        materialized String MATERIALIZED toString(t),
        INDEX tuple_idx t TYPE set(0) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY tuple()
"

for dependency in materialized index
do
    if [ "$dependency" = materialized ]
    then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP INDEX tuple_idx"
        expected="Disable setting 'allow_metadata_only_named_tuple_alter' to run this change as a full mutation"
    else
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DROP COLUMN materialized"
        expected="Disable setting 'allow_metadata_only_named_tuple_alter' to run this change as a full mutation"
    fi

    error=$(${CLICKHOUSE_CLIENT} \
        --allow_metadata_only_named_tuple_alter=1 \
        --query "ALTER TABLE ${TABLE} MODIFY COLUMN t Tuple(a UInt64, b UInt64)" 2>&1 || :)

    echo "$error" | grep -Fq "$expected" && echo OK || echo FAIL

    if [ "$dependency" = materialized ]
    then
        ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} ADD INDEX tuple_idx t TYPE set(0) GRANULARITY 1"
    fi
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"

# One conversion can be enabled by multiple lazy mechanisms. Diagnostics must list all of them.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}
    (
        t Tuple(j JSON(a Int32)),
        materialized String MATERIALIZED toString(t)
    )
    ENGINE = MergeTree
    ORDER BY tuple()
"

error=$(${CLICKHOUSE_CLIENT} \
    --allow_metadata_only_named_tuple_alter=1 \
    --allow_experimental_json_lazy_type_hints=1 \
    --query "ALTER TABLE ${TABLE} MODIFY COLUMN t Tuple(j JSON(a Int64), b UInt64)" 2>&1 || :)

echo "$error" | grep -Fq "Disable settings 'allow_experimental_json_lazy_type_hints', 'allow_metadata_only_named_tuple_alter' to run this change as a full mutation" \
    && echo OK || echo FAIL

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
