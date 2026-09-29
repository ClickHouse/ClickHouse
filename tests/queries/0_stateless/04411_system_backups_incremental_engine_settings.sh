#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

client_opts=(
  --send_logs_level 'error'
)

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    drop table if exists \`04411_t\`;
    create table \`04411_t\` (key Int) engine=MergeTree() order by tuple() settings disk='s3_disk';
    insert into \`04411_t\` select * from numbers(10);
"

base="S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/${CLICKHOUSE_TEST_UNIQUE_NAME}_base')"
inc="S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/${CLICKHOUSE_TEST_UNIQUE_NAME}_inc')"

# A plain (single-engine) S3 backup: engine_settings is populated.
qid_base=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid_base" -q "BACKUP TABLE \`04411_t\` TO $base"

# An incremental S3 backup writes through the destination engine but also reads the base backup,
# i.e. two engines with potentially different endpoint settings. A flat map cannot represent both,
# so engine_settings must be empty (consistent with how incremental RESTOREs are handled).
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "insert into \`04411_t\` select * from numbers(10, 10)"
qid_inc=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid_inc" -q "BACKUP TABLE \`04411_t\` TO $inc SETTINGS base_backup=$base"

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    select length(engine_settings) > 0 from system.backups where query_id = '$qid_base';
    select length(engine_settings) = 0 from system.backups where query_id = '$qid_inc';
"

# The persisted system.backup_log row reflects the same omission for the incremental backup.
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "system flush logs backup_log"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "
    select length(engine_settings) = 0 from system.backup_log where query_id = '$qid_inc' and status = 'BACKUP_CREATED'
"
