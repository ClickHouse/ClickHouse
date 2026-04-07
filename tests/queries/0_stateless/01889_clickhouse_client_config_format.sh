#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings

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
autodetect_xml_with_leading_whitespace_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.config
autodetect_xml_non_leading_whitespace_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.cfg
autodetect_yaml_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.properties
autodetect_invalid_xml_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.badxml
autodetect_invalid_yaml_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.badyaml

function cleanup()
{
    rm "${config:?}"
    rm "${xml_config:?}"
    rm "${XML_config:?}"
    rm "${conf_config:?}"
    rm "${yml_config:?}"
    rm "${yaml_config:?}"
    rm "${autodetect_xml_with_leading_whitespace_config:?}"
    rm "${autodetect_xml_non_leading_whitespace_config:?}"
    rm "${autodetect_yaml_config:?}"
    rm "${autodetect_invalid_xml_config:?}"
    rm "${autodetect_invalid_yaml_config:?}"
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
cat > "$autodetect_xml_with_leading_whitespace_config" <<EOL

    <config>
        <max_threads>2</max_threads>
    </config>
EOL
cat > "$autodetect_xml_non_leading_whitespace_config" <<EOL
<config>
    <max_threads>2</max_threads>
</config>
EOL
cat > "$autodetect_yaml_config" <<EOL
max_threads: 2
EOL
cat > "$autodetect_invalid_xml_config" <<EOL
<!-- This is a XML file comment -->
<invalid tag><invalid tag>
EOL
cat > "$autodetect_invalid_yaml_config" <<EOL
; This is a INI file comment
max_threads: 2
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

echo 'autodetect xml (with leading whitespaces)'
$CLICKHOUSE_CLIENT --config "$autodetect_xml_with_leading_whitespace_config" -q "select getSetting('max_threads')"
echo 'autodetect xml (non leading whitespaces)'
$CLICKHOUSE_CLIENT --config "$autodetect_xml_non_leading_whitespace_config" -q "select getSetting('max_threads')"
echo 'autodetect yaml'
$CLICKHOUSE_CLIENT --config "$autodetect_yaml_config" -q "select getSetting('max_threads')"

# Error code is 1000 (Poco::Exception). It is not ignored.
echo 'autodetect invalid xml'
$CLICKHOUSE_CLIENT --config "$autodetect_invalid_xml_config" -q "select getSetting('max_threads')" 2>&1 |& grep -q "Code: 1000" && echo "Correct: invalid xml parsed with exception" || echo 'Fail: expected error code 1000 but got other'
echo 'autodetect invalid yaml'
$CLICKHOUSE_CLIENT --config "$autodetect_invalid_yaml_config" -q "select getSetting('max_threads')" 2>&1 |& sed -e "s#$CLICKHOUSE_TMP##" -e "s#DB::Exception: ##"