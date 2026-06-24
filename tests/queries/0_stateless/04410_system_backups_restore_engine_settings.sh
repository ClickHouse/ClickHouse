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
    drop table if exists \`04410_t\`;
    create table \`04410_t\` (key Int) engine=MergeTree() order by tuple() settings disk='s3_disk';
    insert into \`04410_t\` select * from numbers(10);
"

$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" -q "BACKUP TABLE \`04410_t\` TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data')"

# Two restores that differ only in the requested allow_s3_native_copy value.
rid_native="${CLICKHOUSE_TEST_UNIQUE_NAME}_rn"
rid_no_native="${CLICKHOUSE_TEST_UNIQUE_NAME}_rnn"

$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" -q "RESTORE TABLE \`04410_t\` AS \`04410_t_nc\` FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data') SETTINGS id='$rid_native', allow_s3_native_copy=true"
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" -q "RESTORE TABLE \`04410_t\` AS \`04410_t_nnc\` FROM S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/data') SETTINGS id='$rid_no_native', allow_s3_native_copy=false"

# For an S3 restore, engine_settings must be populated (not empty) and expose the effective
# S3 request settings, including \`allow_native_copy\`. The effective value must reflect the
# requested allow_s3_native_copy, so the two restores must report different values.
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    select length(engine_settings) > 0 and mapContains(engine_settings, 'allow_native_copy') from system.backups where id = '$rid_native';
    select length(engine_settings) > 0 and mapContains(engine_settings, 'allow_native_copy') from system.backups where id = '$rid_no_native';
    select
        (select engine_settings['allow_native_copy'] from system.backups where id = '$rid_native')
        != (select engine_settings['allow_native_copy'] from system.backups where id = '$rid_no_native');
"
