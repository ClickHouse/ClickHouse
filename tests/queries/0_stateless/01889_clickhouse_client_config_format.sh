#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# use $CLICKHOUSE_DATABASE so that clickhouse-test will replace it with default to match .reference
config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE
xml_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.xml
XML_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.XML
conf_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.conf
yml_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.yml
yaml_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.yaml
ini_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.ini

function cleanup()
{
    rm "${config:?}"
    rm "${xml_config:?}"
    rm "${XML_config:?}"
    rm "${conf_config:?}"
    rm "${yml_config:?}"
    rm "${yaml_config:?}"
    rm "${ini_config:?}"
}
trap cleanup EXIT

cat > "$config" <<EOL
<config>
    <max_threads>2</max_threads>
</config>
EOL
cat > "$conf_config" <<EOL
<config>
    <max_threads>2</max_threads>
</config>
EOL
cat > "$xml_config" <<EOL
<config>
    <max_threads>2</max_threads>
</config>
EOL
cat > "$XML_config" <<EOL
<config>
    <max_threads>2</max_threads>
</config>
EOL
cat > "$yml_config" <<EOL
max_threads: 2
EOL
cat > "$yaml_config" <<EOL
max_threads: 2
EOL
cat > "$ini_config" <<EOL
[config]
max_threads=2
EOL

echo 'default'
$CLICKHOUSE_CLIENT --config "$config" -q "select getSetting('max_threads')"
echo 'xml'
$CLICKHOUSE_CLIENT --config "$xml_config" -q "select getSetting('max_threads')"
echo 'XML'
$CLICKHOUSE_CLIENT --config "$XML_config" -q "select getSetting('max_threads')"
echo 'conf'
$CLICKHOUSE_CLIENT --config "$conf_config" -q "select getSetting('max_threads')"
echo '/dev/fd/PIPE'
# verify that /dev/fd/X parsed as XML (regardless it has .xml extension or not)
# and that pipe does works
$CLICKHOUSE_CLIENT --config <(echo '<config><max_threads>2</max_threads></config>') -q "select getSetting('max_threads')"

echo 'yml'
$CLICKHOUSE_CLIENT --config "$yml_config" -q "select getSetting('max_threads')"
echo 'yaml'
$CLICKHOUSE_CLIENT --config "$yaml_config" -q "select getSetting('max_threads')"
echo 'ini'
$CLICKHOUSE_CLIENT --config "$ini_config" -q "select getSetting('max_threads')" 2>&1 |& sed -e "s#$CLICKHOUSE_TMP##" -e "s#DB::Exception: ##"
