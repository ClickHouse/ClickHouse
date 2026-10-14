#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Settings profiles and constraints from `users.xml` arrive as strings and are converted by
# `BaseSettings::stringToValueUtil`. A rejected value has to name the setting there too, and a URI value
# may carry basic-auth credentials, which must not be echoed back in the error message.
# `clickhouse-local` loads a users config through the same parser as the server, so it is used here: the
# config file below points `users_config` at itself, so one file carries both the server and the users config.

users_config="$(realpath "${CLICKHOUSE_TMP}")/users_${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"

run()
{
    echo "--- $1"
    cat > "$users_config" <<XML
<clickhouse>
    <users_config>$users_config</users_config>
    <profiles>
        <default>
            $2
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas><default></default></quotas>
</clickhouse>
XML
    ${CLICKHOUSE_LOCAL} --config-file "$users_config" --query "SELECT 1" 2>&1 \
        | grep -m1 -oE 'Code: [0-9]+\. DB::Exception: .*' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' -e 's/ (version .*//' \
              -e 's/unsigned long long/unsigned long/'
}

echo '=== a value of the wrong type or out of range in a profile names the setting and the value'
run 'wrong type' '<max_threads>abc</max_threads>'
run 'out of range' '<max_threads>-1</max_threads>'

echo
echo '=== a malformed URI in a profile names the setting, and a password in the value is masked'
run 'malformed URI' '<format_avro_schema_registry_url>http://[</format_avro_schema_registry_url>'
run 'malformed URI with a password' '<format_avro_schema_registry_url>http://user:s3cret@[</format_avro_schema_registry_url>'
echo "password echoed: $(${CLICKHOUSE_LOCAL} --config-file "$users_config" --query "SELECT 1" 2>&1 | grep -c 's3cret')"

echo
echo '=== a malformed URI in a constraint is reported the same way'
echo "--- constraint with a password"
cat > "$users_config" <<XML
<clickhouse>
    <users_config>$users_config</users_config>
    <profiles>
        <default>
            <constraints>
                <format_avro_schema_registry_url>
                    <min>http://user:s3cret@[</min>
                </format_avro_schema_registry_url>
            </constraints>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas><default></default></quotas>
</clickhouse>
XML
${CLICKHOUSE_LOCAL} --config-file "$users_config" --query "SELECT 1" 2>&1 \
    | grep -m1 -oE 'Code: [0-9]+\. DB::Exception: .*' \
    | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' -e 's/ (version .*//'
echo "password echoed: $(${CLICKHOUSE_LOCAL} --config-file "$users_config" --query "SELECT 1" 2>&1 | grep -c 's3cret')"

echo
echo '=== a profile with a fine value still works'
run 'fine value' '<max_threads>4</max_threads>'
${CLICKHOUSE_LOCAL} --config-file "$users_config" --query "SELECT 1"

rm -f "$users_config"
