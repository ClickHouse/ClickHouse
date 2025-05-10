#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/03448_headers_conf.xml
config_path_tmp=$config_path.tmp

# Create temporary config
cat > "$config_path" <<EOF
<?xml version="1.0"?>
<clickhouse>
    <http_handlers>
        <common_http_response_headers>
            <X-My-Common-Header>Common header present</X-My-Common-Header>
        </common_http_response_headers>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/static</url>
            <handler>
                <type>static</type>
                <response_expression>config://http_server_default_response</response_expression>
                <content_type>text/html; charset=UTF-8</content_type>
                <http_response_headers>
                    <X-My-Answer>Iam static</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/ping</url>
            <handler>
                <type>ping</type>
                <http_response_headers>
                    <X-My-Answer>Iam ping</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/replicas_status</url>
            <handler>
                <type>replicas_status</type>
                <http_response_headers>
                    <X-My-Answer>Iam replicas_status</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/play</url>
            <handler>
                <type>play</type>
                <http_response_headers>
                    <X-My-Answer>Iam play</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/dashboard</url>
            <handler>
                <type>dashboard</type>
                <http_response_headers>
                    <X-My-Answer>Iam dashboard</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/binary</url>
            <handler>
                <type>binary</type>
                <http_response_headers>
                    <X-My-Answer>Iam binary</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/merges</url>
            <handler>
                <type>merges</type>
                <http_response_headers>
                    <X-My-Answer>Iam merges</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/metrics</url>
            <handler>
                <type>prometheus</type>
                <http_response_headers>
                    <X-My-Answer>Iam metrics</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/js/lz-string.js</url>
            <handler>
                <type>js</type>
                <http_response_headers>
                    <X-My-Answer>Iam js/lz-string</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,HEAD</methods>
            <url>/js/uplot.js</url>
            <handler>
                <type>js</type>
                <http_response_headers>
                    <X-My-Answer>Iam js/uplot.js</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/query_param_with_url</url>
            <methods>GET,HEAD</methods>
            <headers>
                <PARAMS_XXX><![CDATA[regex:(?P<name_1>[^/]+)]]></PARAMS_XXX>
            </headers>
            <handler>
                <type>predefined_query_handler</type>
                <query>
                    SELECT {name_1:String}
                </query>
                <http_response_headers>
                    <X-My-Answer>Iam predefined</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,POST,HEAD,OPTIONS</methods>
            <handler>
                <type>dynamic_query_handler</type>
                <query_param_name>query</query_param_name>
                <http_response_headers>
                    <X-My-Answer>Iam dynamic</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>
    </http_handlers>
</clickhouse>
EOF

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" 2>&1

for endpoint in static ping replicas_status play dashboard binary merges metrics "js/lz-string.js" "js/uplot.js" "?query=SELECT%201"; do
  ${CLICKHOUSE_CURL} -I "http://localhost:8123/$endpoint" 2>&1 | grep -i 'X-My-Answer';
  ${CLICKHOUSE_CURL} -I "http://localhost:8123/$endpoint" 2>&1 | grep -i 'X-My-Common-Header';
done

${CLICKHOUSE_CURL} -I -H 'PARAMS_XXX:test_param' "http://localhost:8123/query_param_with_url" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I -H 'PARAMS_XXX:test_param' "http://localhost:8123/query_param_with_url" 2>&1 | grep -i 'X-My-Common-Header'

