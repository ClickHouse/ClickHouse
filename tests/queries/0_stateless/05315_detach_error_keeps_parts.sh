#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: the non-transactional DETACH of plain MergeTree on a local blob disk
#
# A DETACH PART / DETACH PARTITION that fails midway (here: an injected memory fault) must not lose a
# part: after a reload of the table from disk, every part is still in the table or has a copy in
# detached/, whether the DETACH succeeded or not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

query "DROP TABLE IF EXISTS t_detach_failure"
# No merge may rename a part, not even after the reload.
query "CREATE TABLE t_detach_failure (a UInt32, b String, c Array(UInt64), d Nullable(Float64))
    ENGINE = MergeTree PARTITION BY a % 2 ORDER BY a
    SETTINGS disk = 'local_disk_3', min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1"

probabilities=(0.005 0.01 0.02 0.05)
lost=0
checked=0
some_failed=0
some_succeeded=0

for i in $(seq 0 39); do
    query "TRUNCATE TABLE t_detach_failure"
    query "ALTER TABLE t_detach_failure DROP DETACHED PARTITION ALL SETTINGS allow_drop_detached = 1"

    # 3 rows in 2 partitions and 3 parts; distinct values per iteration, so that nothing is deduplicated.
    query "INSERT INTO t_detach_failure SETTINGS async_insert = 0, insert_deduplicate = 0
        VALUES ($((3 * i)), 'a', [1, 2], 1.5), ($((3 * i + 1)), 'b', [], NULL)"
    query "INSERT INTO t_detach_failure SETTINGS async_insert = 0, insert_deduplicate = 0
        VALUES ($((3 * i + 2)), 'c', [3], 2.5)"

    parts=$(query "SELECT arrayStringConcat(arraySort(groupArray(name)), ',') FROM system.parts WHERE database = currentDatabase() AND table = 't_detach_failure' AND active")

    if (( i % 2 )); then
        detach="ALTER TABLE t_detach_failure DETACH PARTITION ALL"
    else
        detach="ALTER TABLE t_detach_failure DETACH PART '${parts%%,*}'"
    fi

    code=$(${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}' \
        "${CLICKHOUSE_URL}&memory_tracker_fault_probability=${probabilities[i % 4]}&max_untracked_memory=524288" \
        -d "$detach" 2>/dev/null)
    if [ "$code" = "200" ]; then some_succeeded=1; else some_failed=1; fi

    query "DETACH TABLE t_detach_failure"
    query "ATTACH TABLE t_detach_failure"

    missing=$(query "SELECT count() FROM (SELECT arrayJoin(splitByChar(',', '$parts')) AS p)
        WHERE p NOT IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_detach_failure' AND active)
          AND p NOT IN (SELECT name FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_detach_failure')")
    lost=$((lost + missing))
    checked=$((checked + $(tr ',' '\n' <<< "$parts" | wc -l)))
done

echo "lost $lost"
echo "checked parts $checked"
echo "some DETACH failed $some_failed"
echo "some DETACH succeeded $some_succeeded"

query "DROP TABLE t_detach_failure"
