#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The ALTER MODIFY String -> Nullable(String) metadata-only optimization for a
# Default-serialized part (part 1: no data rewrite, read_rows = 0) is best-effort and is
# very rarely skipped under concurrency on the shared CI server (part 1 gets fully rewritten,
# read_rows = 10000). Data results are always correct; only the optimization-detail assertion
# flips. Retry the scenario until the optimization fires -- a real regression (the optimization
# stops firing) fails every attempt because the decision is deterministic for these inputs.

result=
for attempt in {1..10}; do
    # Unique table per attempt so system.part_log stays isolated (a failed attempt leaves a
    # 1_1_1_0_3 MutatePart row that would otherwise collide with the next attempt's).
    tbl="t_modify_to_nullable_${attempt}"

    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${tbl};

        CREATE TABLE ${tbl} (key UInt64, id UInt64, s String)
        ENGINE = MergeTree ORDER BY id PARTITION BY key
        SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9, auto_statistics_types = '';

        INSERT INTO ${tbl} SELECT 1, number, 'foo' FROM numbers(10000);
        INSERT INTO ${tbl} SELECT 2, number, if (number % 23 = 0, 'bar', '') FROM numbers(10000);
    "

    before=$(${CLICKHOUSE_CLIENT} --query "
        SELECT name, type, serialization_kind FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${tbl}' AND column = 's' AND active
        ORDER BY name;

        SELECT count(s), countIf(s != ''), arraySort(groupUniqArray(s)) FROM ${tbl};
    ")

    ${CLICKHOUSE_CLIENT} --query "
        SET mutations_sync = 2;
        ALTER TABLE ${tbl} MODIFY COLUMN s Nullable(String);
    "

    after=$(${CLICKHOUSE_CLIENT} --query "
        SELECT name, type, serialization_kind FROM system.parts_columns
        WHERE database = currentDatabase() AND table = '${tbl}' AND column = 's' AND active
        ORDER BY name;

        SELECT count(s), countIf(s != ''), arraySort(groupUniqArray(s)) FROM ${tbl};
    ")

    # The synchronous mutation may return before its part_log entry is written, so retry the
    # flush until both MutatePart rows are visible.
    part_log=
    for _ in {1..10}; do
        ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS part_log"
        part_log=$(${CLICKHOUSE_CLIENT} --query "
            SELECT part_name, read_rows FROM system.part_log
            WHERE event_date >= yesterday() AND event_time >= now() - 600 AND database = currentDatabase() AND table = '${tbl}' AND event_type = 'MutatePart'
            ORDER BY part_name")
        [[ $(echo "$part_log" | grep -c .) -eq 2 ]] && break
        sleep 1.0
    done

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${tbl}"

    result="${before}
${after}
${part_log}"

    # Optimization fired iff part 1 was not rewritten (read_rows = 0).
    echo "$part_log" | grep -qP '^1_1_1_0_[0-9]+\t0$' && break
    sleep 1.0
done

echo "$result"
