#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}?query=select" -X POST -F 'query=" 1;"' 2>/dev/null

echo -ne '1,Hello\n2,World\n' | ${CLICKHOUSE_CURL} -sSF 'file=@-' "${CLICKHOUSE_URL}?file_format=CSV&file_types=UInt8,String&query=SELE" -X POST -F 'query="CT * FROM file"' 2>/dev/null
