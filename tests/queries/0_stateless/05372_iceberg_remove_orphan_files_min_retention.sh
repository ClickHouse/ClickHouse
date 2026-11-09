#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)

# `remove_orphan_files` rejects an `older_than` closer to now than `iceberg_orphan_files_min_retention_seconds`,
# because the files of a write that has not committed yet look like orphans.
# https://github.com/ClickHouse/ClickHouse/issues/125331

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --session_timezone=UTC --allow_insert_into_iceberg=1 --allow_iceberg_remove_orphan_files=1 --allow_experimental_iceberg_compaction=0"
TABLE="t_${CLICKHOUSE_DATABASE}_orphan_min_retention"
TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/${TABLE}/"
trap 'rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"' EXIT

$CH -q "CREATE TABLE ${TABLE} (id Int32) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS allow_experimental_iceberg_compaction = 0"
$CH -q "INSERT INTO ${TABLE} VALUES (1)"
recent=$($CH -q "SELECT toString(now() - INTERVAL 1 HOUR)")

# Prints "refused" iff the error is BAD_ARGUMENTS naming the setting, "accepted" on success, else the raw output.
run()
{
    local out
    if out=$($CH "$@" 2>&1); then
        echo accepted
    elif grep -q "BAD_ARGUMENTS" <<<"$out" && grep -q "iceberg_orphan_files_min_retention_seconds" <<<"$out"; then
        echo refused
    else
        echo "$out"
    fi
}

echo "explicit older_than 1 hour ago: $(run -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '${recent}')")"
echo "positional older_than 1 hour ago, dry_run: $(run -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('${recent}', dry_run = 1)")"
echo "default older_than from a 1 hour setting: $(run --iceberg_orphan_files_older_than_seconds=3600 -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files()")"
echo "default older_than from a 1 hour setting, compatibility 26.9: $(run --compatibility=26.9 --iceberg_orphan_files_older_than_seconds=3600 -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files()")"
echo "min retention 2 hours: $(run --iceberg_orphan_files_min_retention_seconds=7200 -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('${recent}')")"
echo "min retention 30 minutes: $(run --iceberg_orphan_files_min_retention_seconds=1800 -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('${recent}')")"
echo "min retention disabled: $(run --iceberg_orphan_files_min_retention_seconds=0 -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('${recent}')")"
echo "older_than in 2000: $(run -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files('2000-01-01 00:00:00')")"
echo "default older_than: $(run -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files()")"

$CH -q "SELECT count() FROM ${TABLE}"
$CH -q "DROP TABLE ${TABLE}"
