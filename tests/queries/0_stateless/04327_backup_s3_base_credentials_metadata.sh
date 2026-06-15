#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

# The `base_backup` locator in `.backup` metadata must not store `S3` credentials.
# Old metadata with embedded credentials remains readable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

client_opts=(
    --allow_repeated_settings
    --send_logs_level 'error'
)

function s3_url() { echo "http://localhost:11111/test/backups/$CLICKHOUSE_DATABASE/base_credentials_metadata_$*"; }

function s3_location() { echo "'$(s3_url "$*")', 'test', 'testtest'"; }

function check_base_backup_in_metadata()
{
    local name=$1 && shift
    local metadata
    # Match only credential arguments, not the `/test/` URL path segment.
    metadata=$($CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT line FROM s3($(s3_location $name/.backup), 'LineAsString') FORMAT TSVRaw")
    grep -c '<base_backup>' <<< "$metadata"
    grep -c '<use_same_s3_credentials_for_base_backup>' <<< "$metadata" || true
    grep -cE ", 'test'|, 'testtest'|SECRET" <<< "$metadata" || true
}

function rewrite_backup_metadata()
{
    local name=$1 && shift
    local from=$1 && shift
    local to=$1 && shift
    local content
    content=$($CLICKHOUSE_CLIENT "${client_opts[@]}" --param_from="$from" --param_to="$to" -q "SELECT replace(line, {from:String}, {to:String}) FROM s3($(s3_location $name/.backup), 'LineAsString')") || return 1
    $CLICKHOUSE_CLIENT "${client_opts[@]}" --param_content="$content" -q "INSERT INTO FUNCTION s3($(s3_location $name/.backup), 'LineAsString') SETTINGS s3_truncate_on_insert=1 VALUES ({content:String})"
}

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    DROP TABLE IF EXISTS data;
    CREATE TABLE data (key Int) ENGINE=MergeTree() ORDER BY tuple();
    INSERT INTO data SELECT * FROM numbers(10);
"

echo 'base'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location base))" | cut -f2

echo 'inc_1: use_same_s3_credentials_for_base_backup'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_1)) SETTINGS base_backup=S3($(s3_location base)), use_same_s3_credentials_for_base_backup=1" | cut -f2
check_base_backup_in_metadata inc_1
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_1 FROM S3($(s3_location inc_1))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_1"

echo 'inc_2: explicit base backup credentials without the setting'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_2)) SETTINGS base_backup=S3($(s3_location base), role_arn = 'SECRET_ROLE_ARN', external_id = 'SECRET_EXTERNAL_ID')" | cut -f2
check_base_backup_in_metadata inc_2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_2 FROM S3($(s3_location inc_2))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_2"

echo 'inc_3: backward compatibility with credentials embedded in metadata'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_3)) SETTINGS base_backup=S3($(s3_location base))" | cut -f2
# Imitate old metadata with embedded credentials and no marker.
rewrite_backup_metadata inc_3 '<use_same_s3_credentials_for_base_backup>true</use_same_s3_credentials_for_base_backup>' ''
rewrite_backup_metadata inc_3 "S3('$(s3_url base)')" "S3('$(s3_url base)', 'test', 'INVALID_PASSWORD')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" --format Null -q "RESTORE TABLE data AS data_3_bad FROM S3($(s3_location inc_3))" |& grep -m1 -o 'The request signature we calculated does not match the signature you provided. Check your key and signing method. (S3_ERROR)'
rewrite_backup_metadata inc_3 'INVALID_PASSWORD' 'testtest'
check_base_backup_in_metadata inc_3
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_3 FROM S3($(s3_location inc_3))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_3"
