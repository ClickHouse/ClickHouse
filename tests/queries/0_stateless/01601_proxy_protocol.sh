#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

printf "PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n\0\21ClickHouse client\24\r\253\251\3\0\7default\0\4\1\0\1\0\0\t0.0.0.0:0\1\tmilovidov\21milovidov-desktop\vClickHouse \24\r\253\251\3\0\1\0\0\0\2\1\25SELECT 'Hello, world'\2\0\247\203\254l\325\\z|\265\254F\275\333\206\342\24\202\24\0\0\0\n\0\0\0\240\1\0\2\377\377\377\377\0\0\0" | nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP_WITH_PROXY}" | head -c150 | grep --text -o -F 'Hello, world'
