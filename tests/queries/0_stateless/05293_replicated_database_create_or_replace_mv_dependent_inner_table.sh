#!/usr/bin/env bash
# Tags: zookeeper

# `CREATE OR REPLACE` of a view in a `Replicated` database whose inner table has a dependent fails before changing anything,
# so the metadata in Keeper still lists exactly the tables of the database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE_1}"
ZK_PATH="/test/${CLICKHOUSE_DATABASE}/05293"
CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --ignore_drop_queries_probability=0"

${CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC"
${CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"
${CLIENT} -q "CREATE TABLE ${DB}.src (x Int64) ENGINE = MergeTree ORDER BY x"
${CLIENT} -q "CREATE MATERIALIZED VIEW ${DB}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM ${DB}.src"
INNER=$(${CLIENT} -q "SELECT name FROM system.tables WHERE database = '${DB}' AND name LIKE '.inner_id.%'")
${CLIENT} -q "CREATE DICTIONARY ${DB}.dict (x Int64) PRIMARY KEY x SOURCE(CLICKHOUSE(DB '${DB}' TABLE '${INNER}')) LAYOUT(FLAT()) LIFETIME(0)"

function check_replace()
{
    ${CLIENT} "$@" 2>&1 | grep -o -m1 'HAVE_DEPENDENT_OBJECTS'
    ${CLIENT} -q "
        SELECT (SELECT arraySort(groupArray(decodeURLComponent(name))) FROM system.zookeeper WHERE path = '${ZK_PATH}/metadata')
            = (SELECT arraySort(groupArray(name)) FROM system.tables WHERE database = '${DB}')"
}

check_replace -q "CREATE OR REPLACE MATERIALIZED VIEW ${DB}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM ${DB}.src"
${CLIENT} -q "DROP DICTIONARY ${DB}.dict SYNC"

# The new table itself depends on the replaced inner table.
check_replace --check_referential_table_dependencies=1 -q "
    CREATE OR REPLACE MATERIALIZED VIEW ${DB}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM ${DB}.\`${INNER}\`"
check_replace -q "
    CREATE OR REPLACE TABLE ${DB}.mv (x Int64, CONSTRAINT c CHECK x >= (SELECT count() FROM ${DB}.\`${INNER}\`)) ENGINE = MergeTree ORDER BY x"

${CLIENT} -q "DROP DATABASE ${DB} SYNC"
