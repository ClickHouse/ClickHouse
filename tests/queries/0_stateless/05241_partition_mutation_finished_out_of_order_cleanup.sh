#!/usr/bin/env bash
#
# With partition-scoped (`IN PARTITION`) mutations the finished mutations of a plain `MergeTree`
# table are no longer a prefix of all mutations ordered by version: a later mutation of one
# partition can finish while an earlier mutation of another partition is still pending. Such a
# later mutation must still be marked finished and removed by the cleanup thread according to
# `finished_mutations_to_keep`, instead of piling up behind the pending one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_05241"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $TABLE"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $TABLE (p UInt8, n Int64)
    ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
    SETTINGS finished_mutations_to_keep = 1, merge_tree_clear_old_parts_interval_seconds = 1,
        cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0"

${CLICKHOUSE_CLIENT} --query "INSERT INTO $TABLE VALUES (1, 1), (2, 2)"

# The mutation of partition 1 fails on its only part, so it stays pending.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE UPDATE n = throwIf(n > 0, 'keep the mutation pending') IN PARTITION 1 WHERE 1"

# Two later mutations of partition 2 finish.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE UPDATE n = n + 10 IN PARTITION 2 WHERE 1"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE UPDATE n = n + 100 IN PARTITION 2 WHERE 1"

# Only the newest finished mutation is kept, the older finished one is removed although the
# pending mutation of partition 1 is older than both.
for _ in {1..300}
do
    count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'")
    [ "$count" -le 2 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query "
    SELECT command, is_done FROM system.mutations
    WHERE database = currentDatabase() AND table = '$TABLE' ORDER BY mutation_id"

${CLICKHOUSE_CLIENT} --query "SELECT p, n FROM $TABLE ORDER BY p"

${CLICKHOUSE_CLIENT} --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$TABLE' SYNC FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $TABLE"
