#!/usr/bin/env bash

# `ABR` executes the query on a source table, where a filter of `additional_table_filters` for the `ABR` table, or for a
# table that reads it, cannot be applied, so such a read is rejected. A filter for a source table applies to the query
# executed on it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --allow_experimental_abr=1 --session_timezone=UTC"

${CLIENT} --query "
    DROP TABLE IF EXISTS abr_alias;
    DROP TABLE IF EXISTS abr;
    DROP TABLE IF EXISTS events_1;
    DROP TABLE IF EXISTS events_10;

    CREATE TABLE events_1 (ts DateTime, account_id UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    CREATE TABLE events_10 (ts DateTime, account_id UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
    INSERT INTO events_1 SELECT toDateTime('2026-01-01 00:00:00') + number, number % 2 FROM numbers(100);
    INSERT INTO events_10 SELECT toDateTime('2026-01-01 00:00:00') + number, number % 2 FROM numbers(100);

    CREATE TABLE abr ENGINE = ABR('events', [1, 10], ('ts'));
    CREATE TABLE abr_alias ENGINE = Alias('abr');
"

query="SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')"
query_aliased="SELECT count() FROM abr AS a WHERE ts >= toDateTime('2026-01-01 00:00:00')"
query_alias_table="SELECT count() FROM abr_alias WHERE ts >= toDateTime('2026-01-01 00:00:00')"

for analyzer in 1 0; do
    echo "enable_analyzer = ${analyzer}"
    for case in \
        "fully qualified key|${query}|${CLICKHOUSE_DATABASE}.abr" \
        "table name key|${query}|abr" \
        "table alias key|${query_aliased}|a" \
        "Alias table key|${query_alias_table}|abr_alias"
    do
        IFS='|' read -r name q key <<< "${case}"
        echo "${name}: $(${CLIENT} --enable_analyzer=${analyzer} --query "${q} SETTINGS additional_table_filters = {'${key}': 'account_id = 1'}" 2>&1 | grep -o 'NOT_IMPLEMENTED' | uniq)"
    done
    echo "source table key: $(${CLIENT} --enable_analyzer=${analyzer} --query "${query} SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.events_1': 'account_id = 1'}")"
    echo "other table key: $(${CLIENT} --enable_analyzer=${analyzer} --query "${query} SETTINGS additional_table_filters = {'other': 'account_id = 1'}")"
done

${CLIENT} --query "
    DROP TABLE abr_alias;
    DROP TABLE abr;
    DROP TABLE events_1;
    DROP TABLE events_10;
"
