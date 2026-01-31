#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test external table with native compressed format using _decompress parameter

# Generate compressed native block and pipe directly to external table query
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&query=SELECT+sum(id)+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1"

# Without decompress flag, compressed data should fail
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native" 2>&1 | \
    grep -c 'Exception' | sed 's/1/error/'
