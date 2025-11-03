#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/query_masking_$db.xml
echo "
<clickhouse>
      <query_masking_rules>
            <rule>
                  <name>hide s3 secret key in named collection</name>
                  <regexp>sensetive</regexp>
                  <replace>replaced</replace>
            </rule>
      </query_masking_rules>
      <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
" > $config_path

${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD CONFIG"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $db.test_table"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE $db.test_table ON CLUSTER 'test_shard_localhost' (s String, sensetive UInt32) ENGINE = MergeTree ORDER BY s" > /dev/null
sleep 1
${CLICKHOUSE_CLIENT} --query "SELECT create_table_query FROM system.tables WHERE database='$db' AND table='test_table' SETTINGS format_display_secrets_in_show_and_select=1;" | grep -c 'sensetive'
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $db.test_table"

rm $config_path
${CLICKHOUSE_CLIENT} --query "SYSTEM RELOAD CONFIG"
