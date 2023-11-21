#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data;
    create table data (key Int) engine=MergeTree() order by tuple();
    insert into data select * from numbers(10);
"

function write_invalid_password_to_base_backup()
{
    local name=$1 && shift
    local content
    content=$($CLICKHOUSE_CLIENT -q "select replace(line, 'testtest', 'INVALID_PASSWORD') from s3($(s3_location $name/.backup), 'LineAsString')") || return 1
    $CLICKHOUSE_CLIENT --param_content="$content" -q "insert into function s3($(s3_location $name/.backup), 'LineAsString') settings s3_truncate_on_insert=1 values ({content:String})"
}

# Returns the arguments for the BACKUP TO S3() function, i.e. (url, access_key_id, secret_access_key)
function s3_location() { echo "'http://localhost:11111/test/backups/$CLICKHOUSE_DATABASE/use_same_s3_credentials_for_base_backup_base_$*', 'test', 'testtest'"; }

echo 'use_same_s3_credentials_for_base_backup for S3'
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO S3($(s3_location base))" | cut -f2
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO S3($(s3_location inc_1)) SETTINGS base_backup=S3($(s3_location base))" | cut -f2
write_invalid_password_to_base_backup inc_1
$CLICKHOUSE_CLIENT --format Null -q "BACKUP TABLE data TO S3($(s3_location inc_2)) SETTINGS base_backup=S3($(s3_location inc_1))" |& grep -m1 -o 'The request signature we calculated does not match the signature you provided. Check your key and signing method. (S3_ERROR)'
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO S3($(s3_location inc_3)) SETTINGS base_backup=S3($(s3_location inc_1)), use_same_s3_credentials_for_base_backup=1" | cut -f2

$CLICKHOUSE_CLIENT --format Null -q "RESTORE TABLE data AS data FROM S3($(s3_location inc_1))" |& grep -m1 -o 'The request signature we calculated does not match the signature you provided. Check your key and signing method. (S3_ERROR)'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE data AS data_1 FROM S3($(s3_location inc_1)) SETTINGS use_same_s3_credentials_for_base_backup=1" | cut -f2
$CLICKHOUSE_CLIENT -q "RESTORE TABLE data AS data_2 FROM S3($(s3_location inc_3)) SETTINGS use_same_s3_credentials_for_base_backup=1" | cut -f2

echo 'use_same_s3_credentials_for_base_backup for S3 (invalid arguments)'
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO S3($(s3_location inc_4_bad)) SETTINGS base_backup=S3($(s3_location inc_1), 'foo'), use_same_s3_credentials_for_base_backup=1" |& cut -f2
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO S3($(s3_location inc_5_bad), 'foo') SETTINGS base_backup=S3($(s3_location inc_1)), use_same_s3_credentials_for_base_backup=1" |& grep -o -m1 NUMBER_OF_ARGUMENTS_DOESNT_MATCH

echo 'use_same_s3_credentials_for_base_backup for Disk'
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO Disk('default', '$CLICKHOUSE_DATABASE/backup_1') SETTINGS use_same_s3_credentials_for_base_backup=1" | cut -f2
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO Disk('default', '$CLICKHOUSE_DATABASE/backup_2') SETTINGS use_same_s3_credentials_for_base_backup=1, base_backup=Disk('default', '$CLICKHOUSE_DATABASE/backup_1')" |& grep -o -m1 BAD_ARGUMENTS

exit 0
