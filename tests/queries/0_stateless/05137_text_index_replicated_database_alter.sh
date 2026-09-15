#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `distributed_ddl_output_mode=none`: suppress the per-replica DDL status rows, so the output
# does not depend on the CI flavor (the DBReplicated flavor sets the setting to `none`).
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --database_replicated_allow_explicit_uuid=1 --distributed_ddl_output_mode=none"
db="${CLICKHOUSE_DATABASE}_05137"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db} SYNC"
}
trap cleanup EXIT
cleanup

expect_bad_arguments()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} -q "$1" 2>&1)
    [[ "$output" == *"Code: 36."* && "$output" == *"(BAD_ARGUMENTS)"* ]]
}

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db} ENGINE = Replicated('/clickhouse/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/replicated_database', 'shard1', 'replica1')"

# The initiating replica validates a new definition as `CREATE`; a follower only replays a
# definition after it has been committed by the initiator.
expect_bad_arguments "
    CREATE TABLE ${db}.invalid
    (
        t Array(Array(String)),
        INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree ORDER BY tuple()" || exit 1

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${db}.tab
    (
        t Array(Array(String))
    )
    ENGINE = MergeTree ORDER BY tuple()"

# Initial `ALTER` DDL also remains strict; a replica must not introduce invalid metadata.
expect_bad_arguments "ALTER TABLE ${db}.tab ADD INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')" || exit 1

# Atomic databases require a UUID for full-definition `ATTACH`. Generate one per execution:
# failed `ATTACH` statements can leave a UUID mapping behind, so a fixed value makes retries flaky.
uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
expect_bad_arguments "
    ATTACH TABLE ${db}.attached UUID '${uuid}'
    (
        t Array(Array(String)),
        INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree ORDER BY tuple()" || exit 1
