#!/usr/bin/env bash
# Tags: zookeeper, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# with Atomic engine
$CLICKHOUSE_CLIENT --query "CREATE USER u1_04010"
$CLICKHOUSE_CLIENT --query "CREATE ROLE r1_04010"

function run_concurrent_grants
{
    for _ in {1..20}; do
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "GRANT r1_04010 TO u1_04010"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "REVOKE r1_04010 FROM u1_04010"
    done
}
export -f run_concurrent_grants

for _ in {1..20}; do
    bash -c run_concurrent_grants &
done

wait

$CLICKHOUSE_CLIENT --query "DROP ROLE r1_04010"
$CLICKHOUSE_CLIENT --query "DROP USER u1_04010"

