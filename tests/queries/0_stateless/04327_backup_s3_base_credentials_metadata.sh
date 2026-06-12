#!/usr/bin/env bash
# Tags: no-fasttest
# Tag: no-fasttest - requires S3

# Regression test for https://github.com/ClickHouse/clickhouse-private/issues/60577:
# the <base_backup> locator written into the .backup metadata file must never contain
# credentials. With use_same_s3_credentials_for_base_backup=1 a non-secret
# <use_same_s3_credentials_for_base_backup> marker is written instead, so on restore the
# credentials are taken from the restore source locator automatically. Without the setting
# the credentials can be given to RESTORE with the base_backup setting. Existing backups
# with credentials embedded in the metadata by older versions remain restorable as is.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

client_opts=(
    --allow_repeated_settings
    --send_logs_level 'error'
)

function s3_url() { echo "http://localhost:11111/test/backups/$CLICKHOUSE_DATABASE/base_credentials_metadata_$*"; }

# Returns the arguments for the BACKUP TO S3() function, i.e. (url, access_key_id, secret_access_key)
function s3_location() { echo "'$(s3_url "$*")', 'test', 'testtest'"; }

# Prints the number of lines of the .backup metadata file mentioning the base backup,
# the number of lines with the use_same_s3_credentials_for_base_backup marker,
# and the number of lines containing credentials. The access key and the secret are matched
# only in credential-argument positions (", 'test'" / ", 'testtest'"), so that the /test/
# path segment of the URL does not cause false positives.
function check_base_backup_in_metadata()
{
    local name=$1 && shift
    local metadata
    # TSVRaw keeps the quotes unescaped, so that the credential-argument patterns match
    metadata=$($CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT line FROM s3($(s3_location $name/.backup), 'LineAsString') FORMAT TSVRaw")
    grep -c '<base_backup>' <<< "$metadata"
    grep -c '<use_same_s3_credentials_for_base_backup>' <<< "$metadata" || true
    grep -cE ", 'test'|, 'testtest'|SECRET" <<< "$metadata" || true
}

# Rewrites the .backup metadata file, replacing $from with $to,
# to imitate a backup created by an older server version
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
# The marker makes the restore take the base backup credentials from the restore source locator,
# so no restore-time settings are needed
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_1 FROM S3($(s3_location inc_1))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_1"

echo 'inc_2: explicit base backup credentials without the setting'
# The key-value authentication arguments must not be persisted in the metadata either
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_2)) SETTINGS base_backup=S3($(s3_location base), role_arn = 'SECRET_ROLE_ARN', external_id = 'SECRET_EXTERNAL_ID')" | cut -f2
check_base_backup_in_metadata inc_2
# The credentials are not in the metadata and there is no marker, so they are given to RESTORE explicitly
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_2 FROM S3($(s3_location inc_2)) SETTINGS base_backup=S3($(s3_location base))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_2"

echo 'inc_3: backward compatibility with credentials embedded in metadata'
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "BACKUP TABLE data TO S3($(s3_location inc_3)) SETTINGS base_backup=S3($(s3_location base))" | cut -f2
# Embed invalid credentials into the metadata, as if the backup was created by an older server
# version, and check they are used (and not, e.g., some server-side credentials)
rewrite_backup_metadata inc_3 "S3('$(s3_url base)')" "S3('$(s3_url base)', 'test', 'INVALID_PASSWORD')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" --format Null -q "RESTORE TABLE data AS data_3_bad FROM S3($(s3_location inc_3))" |& grep -m1 -o 'The request signature we calculated does not match the signature you provided. Check your key and signing method. (S3_ERROR)'
# With valid embedded credentials such a backup is restorable without any restore-time settings
rewrite_backup_metadata inc_3 'INVALID_PASSWORD' 'testtest'
check_base_backup_in_metadata inc_3
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "RESTORE TABLE data AS data_3 FROM S3($(s3_location inc_3))" | cut -f2
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "SELECT count() FROM data_3"
