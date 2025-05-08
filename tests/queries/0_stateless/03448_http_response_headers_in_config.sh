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
        <rule>
            <url>/static</url>
            <methods>GET,HEAD</methods>
            <handler>
                <type>static</type>
                <response_expression>config://http_server_default_response</response_expression>
                <content_type>text/html; charset=UTF-8</content_type>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/ping</url>
            <methods>GET</methods>
            <handler>
                <type>ping</type>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/replicas_status</url>
            <methods>GET,HEAD</methods>
            <handler>
                <type>replicas_status</type>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/play</url>
            <methods>GET,HEAD</methods>
            <handler>
                <type>play</type>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/dashboard</url>
            <methods>GET,HEAD</methods>
            <handler>
                <type>dashboard</type>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/binary</url>
            <methods>GET,HEAD</methods>
            <handler>
                <type>binary</type>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <url>/merges</url>
            <methods>GET,HEAD</methods>
            <handler>
                <type>merges</type>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>

        <rule>
            <methods>GET,POST,HEAD,OPTIONS</methods>
            <handler>
                <type>dynamic_query_handler</type>
                <query_param_name>query</query_param_name>
                <http_response_headers>
                    <X-My-Answer>42</X-My-Answer>
                </http_response_headers>
            </handler>
        </rule>
    </http_handlers>
</clickhouse>
EOF

$CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" 2>&1

${CLICKHOUSE_CURL} -I "http://localhost:8123/static" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I "http://localhost:8123/ping" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I "http://localhost:8123/replicas_status" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I "http://localhost:8123/play" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I "http://localhost:8123/dashboard" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I "http://localhost:8123/binary" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I "http://localhost:8123/merges" 2>&1 | grep -i 'X-My-Answer'
${CLICKHOUSE_CURL} -I "http://localhost:8123/?query=SELECT%201" 2>&1 | grep -i 'X-My-Answer'
