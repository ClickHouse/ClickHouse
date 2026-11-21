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

function s3_location_root() { echo "'$(s3_url "$*")', 'clickhouse', 'clickhouse'"; }

function check_base_backup_in_metadata()
{
    local name=$1 && shift
    local metadata
    # Match only credential arguments, not the `/test/` URL path segment.
    metadata=$($CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT line FROM s3($(s3_location $name/.backup), 'LineAsString') FORMAT TSVRaw")
    grep -c '<base_backup>' <<< "$metadata" || true
    grep -c '<base_backup_copy_s3_credentials_from_backup>' <<< "$metadata" || true
    grep -c '<use_same_s3_credentials_for_base_backup>' <<< "$metadata" || true
    grep -cE ", 'test'|, 'testtest'|, 'clickhouse'|SECRET" <<< "$metadata" || true
}

function expect_restore_failure()
{
    local query=$1 && shift
    if $CLICKHOUSE_CLIENT "${client_opts[@]}" -q "$query" > /dev/null 2>&1; then
        echo 'RESTORE_UNEXPECTEDLY_SUCCEEDED'
    else
        echo 'RESTORE_FAILED'
    fi
}

function rewrite_backup_metadata()
{
    local name=$1 && shift
    local from=$1 && shift
    local to=$1 && shift
    local content
    content=$($CLICKHOUSE_CLIENT "${client_opts[@]}" --param_from="$from" --param_to="$to" -q "SELECT replace(line, {from:String}, {to:String}) FROM s3($(s3_location $name/.backup), 'LineAsString') FORMAT LineAsString") || return 1
    $CLICKHOUSE_CLIENT "${client_opts[@]}" -q "INSERT INTO FUNCTION s3($(s3_location $name/.backup), 'LineAsString') SETTINGS s3_truncate_on_insert=1 FORMAT LineAsString" <<< "$content"
}

$CLICKHOUSE_CLIENT "${client_opts[@]}" -m -q "
    DROP TABLE IF EXISTS data;
    CREATE TABLE data (key Int) ENGINE=MergeTree() ORDER BY tuple();
    INSERT INTO data SELECT * FROM numbers(10);
"

echo 'base'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location base))" | cut -f2
check_base_backup_in_metadata base

echo 'inc_1: use_same_s3_credentials_for_base_backup'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_1)) SETTINGS base_backup=S3($(s3_location base)), use_same_s3_credentials_for_base_backup=1" | cut -f2
check_base_backup_in_metadata inc_1
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_1 FROM S3($(s3_location inc_1))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_1"
expect_restore_failure "RESTORE TABLE data AS data_1_wrong_base FROM S3($(s3_location inc_1)) SETTINGS base_backup=S3('$(s3_url base)', 'WRONG', 'WRONG')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_1_explicit_base FROM S3($(s3_location inc_1)) SETTINGS base_backup=S3($(s3_location base))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_1_explicit_base"

echo 'inc_2: explicit base backup credentials without the setting'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_2)) SETTINGS base_backup=S3($(s3_location base)), use_same_s3_credentials_for_base_backup=0" | cut -f2
check_base_backup_in_metadata inc_2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_2 FROM S3($(s3_location inc_2))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_2"

echo 'inc_3: different inline base credentials without the setting'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_3)) SETTINGS base_backup=S3($(s3_location_root base))" | cut -f2
check_base_backup_in_metadata inc_3
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_3 FROM S3($(s3_location inc_3)) SETTINGS base_backup=S3($(s3_location_root base))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_3"

echo 'inc_4: distinct base credentials and extra auth arguments'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_4)) SETTINGS base_backup=S3($(s3_location_root base), role_arn = 'SECRET_ROLE_ARN', external_id = 'SECRET_EXTERNAL_ID')" | cut -f2
check_base_backup_in_metadata inc_4
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_4 FROM S3($(s3_location inc_4)) SETTINGS base_backup=S3($(s3_location_root base))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_4"

echo 'inc_5: use_same_s3_credentials with extra auth arguments'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_5)) SETTINGS base_backup=S3('$(s3_url base)', extra_credentials(external_id = 'SECRET_EXTERNAL_ID')), use_same_s3_credentials_for_base_backup=1" | cut -f2
check_base_backup_in_metadata inc_5
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_5 FROM S3($(s3_location inc_5)) SETTINGS base_backup=S3($(s3_location base))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_5"

echo 'inc_6: backward compatibility with credentials embedded in metadata'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_6)) SETTINGS base_backup=S3($(s3_location base))" | cut -f2
# Imitate old metadata with embedded credentials and no marker.
rewrite_backup_metadata inc_6 '<base_backup_copy_s3_credentials_from_backup>true</base_backup_copy_s3_credentials_from_backup>' ''
rewrite_backup_metadata inc_6 '<use_same_s3_credentials_for_base_backup>true</use_same_s3_credentials_for_base_backup>' ''
rewrite_backup_metadata inc_6 "S3('$(s3_url base)')" "S3('$(s3_url base)', 'test', 'testtest')"
check_base_backup_in_metadata inc_6
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_6 FROM S3($(s3_location inc_6))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_6"

echo 'inc_7: google ADC secrets in base credentials are redacted'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_7)) SETTINGS base_backup=S3($(s3_location_root base), google_adc_client_secret = 'SECRET_ADC_CLIENT_SECRET', google_adc_refresh_token = 'SECRET_ADC_REFRESH_TOKEN')" | cut -f2
check_base_backup_in_metadata inc_7
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_7 FROM S3($(s3_location inc_7)) SETTINGS base_backup=S3($(s3_location_root base))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_7"
