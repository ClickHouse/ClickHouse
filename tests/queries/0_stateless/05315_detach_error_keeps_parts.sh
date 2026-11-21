#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: the non-transactional DETACH of plain MergeTree on a local blob disk
#
# A DETACH PART / DETACH PARTITION that fails midway (here: an injected memory fault) must not lose a
# part: every part is still in the table or has a copy in detached/, whether the DETACH succeeded or not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

# A table per DETACH, on a local blob disk of this run's own, with compact parts and no merges.
# Even tables: 1 part, for DETACH PART. Odd tables: 3 parts in 2 partitions, for DETACH PARTITION ALL.
for i in $(seq 0 19); do
    rows="(0, 'a', [1, 2], 1.5)"
    if (( i % 2 )); then rows="$rows, (1, 'b', [], NULL)"; fi
    query "CREATE TABLE t_detach_failure_$i (a UInt32, b String, c Array(UInt64), d Nullable(Float64))
        ENGINE = MergeTree PARTITION BY a % 2 ORDER BY a
        SETTINGS disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/'),
            min_bytes_for_wide_part = 1000000000, max_bytes_to_merge_at_max_space_in_pool = 1
        AS SELECT * FROM values('a UInt32, b String, c Array(UInt64), d Nullable(Float64)', $rows)"
    if (( i % 2 )); then
        query "INSERT INTO t_detach_failure_$i SETTINGS async_insert = 0, insert_deduplicate = 0 VALUES (2, 'c', [3], 2.5)"
    fi
done

query "CREATE TABLE parts_before ENGINE = Memory AS
    SELECT table, name FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_detach_failure_%' AND active"
targets=$(query "SELECT table, min(name) FROM parts_before GROUP BY table FORMAT TSV")

some_failed=0
for i in $(seq 0 19); do
    if (( i % 2 )); then
        detach="ALTER TABLE t_detach_failure_$i DETACH PARTITION ALL"
        probability=0.0125
    else
        detach="ALTER TABLE t_detach_failure_$i DETACH PART '$(awk -v t="t_detach_failure_$i" '$1 == t { print $2 }' <<< "$targets")'"
        probability=0.03
    fi

    code=$(${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}' \
        "${CLICKHOUSE_URL}&memory_tracker_fault_probability=${probability}&max_untracked_memory=524288" \
        -d "$detach" 2>/dev/null)
    if [ "$code" != "200" ]; then some_failed=1; fi
done

read -r lost checked <<< "$(query "SELECT
        countIf((table, name) NOT IN (SELECT table, name FROM system.parts WHERE database = currentDatabase() AND active)
            AND (table, name) NOT IN (SELECT table, name FROM system.detached_parts WHERE database = currentDatabase())),
        count()
    FROM parts_before FORMAT TSV")"
echo "lost $lost"
echo "checked parts $checked"
echo "some DETACH failed $some_failed"
