#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Implementation specific behaviour on overflow. We may return error or produce empty string.
${CLICKHOUSE_CLIENT} --query="SELECT randomPrintableASCII(nan);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT randomPrintableASCII(inf);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT randomPrintableASCII(-inf);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT randomPrintableASCII(1e300);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT randomPrintableASCII(-123.456);" >/dev/null 2>&1 ||:
${CLICKHOUSE_CLIENT} --query="SELECT randomPrintableASCII(-1);" >/dev/null 2>&1 ||:

${CLICKHOUSE_CLIENT} --query="SELECT randomPrintableASCII(0), 'Ok';"
