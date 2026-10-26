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
    drop table if exists \`04339_t\`;
    create table \`04339_t\` (key Int) engine=MergeTree() order by tuple() settings disk='s3_disk';
    insert into \`04339_t\` select * from numbers(10);
"

# Two S3 backups that differ only in the requested allow_s3_native_copy value.
qid_native=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid_native" -q "BACKUP TABLE \`04339_t\` TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/${CLICKHOUSE_TEST_UNIQUE_NAME}_data_native') SETTINGS allow_s3_native_copy=true"

qid_no_native=$(random_str 10)
$CLICKHOUSE_CLIENT --format Null "${client_opts[@]}" --query_id "$qid_no_native" -q "BACKUP TABLE \`04339_t\` TO S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/${CLICKHOUSE_TEST_UNIQUE_NAME}_data_no_native') SETTINGS allow_s3_native_copy=false"

# For an S3 backup, engine_settings must be populated (not empty) and expose the effective
# S3 request settings, including `allow_native_copy`. The effective value must reflect the
# requested allow_s3_native_copy, so the two backups must report different values.
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    select length(engine_settings) > 0 and mapContains(engine_settings, 'allow_native_copy') from system.backups where query_id = '$qid_native';
    select length(engine_settings) > 0 and mapContains(engine_settings, 'allow_native_copy') from system.backups where query_id = '$qid_no_native';
    select
        (select engine_settings['allow_native_copy'] from system.backups where query_id = '$qid_native')
        != (select engine_settings['allow_native_copy'] from system.backups where query_id = '$qid_no_native');
    -- Request settings that backup S3 IO does not actually consume must not be reported (e.g. the delete
    -- batch size, which BackupWriterS3 hardcodes to the S3 API limit, or disk-only settings).
    select not has(mapKeys(engine_settings), 'objects_chunk_size_to_delete')
       and not has(mapKeys(engine_settings), 'list_object_keys_size')
       and not has(mapKeys(engine_settings), 'read_only')
       and not has(mapKeys(engine_settings), 'throw_on_zero_files_match')
    from system.backups where query_id = '$qid_native';
"

# The same must be exposed by the persisted system.backup_log row (exercises the log-specific
# BackupLogElement::appendToBlock path): a settings value plus the non-empty engine_settings shape.
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "system flush logs backup_log"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    select
        settings['allow_s3_native_copy'],
        length(engine_settings) > 0 and mapContains(engine_settings, 'allow_native_copy')
    from system.backup_log where query_id = '$qid_native' and status = 'BACKUP_CREATED'
"
