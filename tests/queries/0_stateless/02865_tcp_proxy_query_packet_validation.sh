#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: nc - command not found

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

printf "PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n\0\21ClickHouse client\24\r\253\251\3\0\7default\0\4\1\0\1\0\0\t0.0.0.0:0\1\6hacker\16hacker-desktop\15Hacker client\24\r\253\251\3\0\1\0\0\0\2\1\25SELECT 'Hello, world'\2\0\247\203\254l\325\\z|\265\254F\275\333\206\342\24\202\24\0\0\0\n\0\0\0\240\1\0\2\377\377\377\377\0\0\0" | nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP_WITH_PROXY}" | head -c250 | grep --text -o -F 'client_name does not match'
printf "PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n\0\21ClickHouse client\24\r\253\251\3\0\7default\0\4\1\0\1\0\0\t0.0.0.0:0\1\6hacker\16hacker-desktop\21ClickHouse client\20\r\253\251\3\0\1\0\0\0\2\1\25SELECT 'Hello, world'\2\0\247\203\254l\325\\z|\265\254F\275\333\206\342\24\202\24\0\0\0\n\0\0\0\240\1\0\2\377\377\377\377\0\0\0" | nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP_WITH_PROXY}" | head -c250 | grep --text -o -F 'version does not match'
