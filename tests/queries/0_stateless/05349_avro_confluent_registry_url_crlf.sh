#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs Avro

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The path of `format_avro_schema_registry_url` must stay percent-encoded in the schema registration
# request, so an encoded CR or LF cannot reach the request line. The server answers with an error
# that echoes the requested path. The default 1 second timeouts are too short for slow builds.
${CLICKHOUSE_CLIENT} --query "
    SELECT 1::UInt8 AS a FORMAT AvroConfluent
    SETTINGS format_avro_schema_registry_url = 'http://localhost:${CLICKHOUSE_PORT_HTTP}/x%0D%0AX-Injected:%201',
        output_format_avro_confluent_subject = 's', format_avro_schema_registry_max_retries = 0,
        format_avro_schema_registry_connection_timeout = 60, format_avro_schema_registry_send_timeout = 60,
        format_avro_schema_registry_receive_timeout = 60" 2>&1 \
    | grep -o -m1 "remote server /[^ ]*/versions"
