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
ini_config=$CLICKHOUSE_TMP/config_$CLICKHOUSE_DATABASE.ini

function cleanup() {
	rm "${config:?}"
	rm "${xml_config:?}"
	rm "${XML_config:?}"
	rm "${conf_config:?}"
	rm "${yml_config:?}"
	rm "${yaml_config:?}"
	rm "${ini_config:?}"
}
trap cleanup EXIT

cat >"$config" <<EOL
<config>
    <openSSL>
        <client>
            <invalidCertificateHandler>
                <name>RejectCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>
</config>
EOL
cat >"$conf_config" <<EOL
<config>
    <openSSL>
        <client>
            <invalidCertificateHandler>
                <name>RejectCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>
</config>
EOL
cat >"$xml_config" <<EOL
<config>
    <openSSL>
        <client>
            <invalidCertificateHandler>
                <name>RejectCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>
</config>
EOL
cat >"$XML_config" <<EOL
<config>
    <openSSL>
        <client>
            <invalidCertificateHandler>
                <name>RejectCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>
</config>
EOL
cat >"$yml_config" <<EOL
openSSL:
  client:
    invalidCertificateHandler:
      name: RejectCertificateHandler
EOL
cat >"$yaml_config" <<EOL
openSSL:
  client:
    invalidCertificateHandler:
      name: RejectCertificateHandler
EOL
cat >"$ini_config" <<EOL
[openSSL.client.invalidCertificateHandler]
name = RejectCertificateHandler
EOL

echo 'default'
$CLICKHOUSE_CLIENT --config "$config" -q "select getSetting('invalidCertificateHandler')"
echo 'xml'
$CLICKHOUSE_CLIENT --config "$xml_config" -q "select getSetting('invalidCertificateHandler')"
echo 'XML'
$CLICKHOUSE_CLIENT --config "$XML_config" -q "select getSetting('invalidCertificateHandler')"
echo 'conf'
$CLICKHOUSE_CLIENT --config "$conf_config" -q "select getSetting('invalidCertificateHandler')"
echo '/dev/fd/PIPE'
# verify that /dev/fd/X parsed as XML (regardless it has .xml extension or not)
# and that pipe does works
$CLICKHOUSE_CLIENT --config <(echo '<config><invalidCertificateHandler><name>RejectCertificateHandler</name></invalidCertificateHandler></config>') -q "select getSetting('invalidCertificateHandler')"

echo 'yml'
$CLICKHOUSE_CLIENT --config "$yml_config" -q "select getSetting('invalidCertificateHandler')"
echo 'yaml'
$CLICKHOUSE_CLIENT --config "$yaml_config" -q "select getSetting('invalidCertificateHandler')"
echo 'ini'
$CLICKHOUSE_CLIENT --config "$ini_config" -q "select getSetting('invalidCertificateHandler')" 2>&1 |& sed -e "s#$CLICKHOUSE_TMP##" -e "s#DB::Exception: ##"
