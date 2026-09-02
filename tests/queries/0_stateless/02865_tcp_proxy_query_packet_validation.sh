#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: nc - command not found

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The server closes the connection right after rejecting these malformed Hello
# packets; the resulting TCP RST can race ahead of nc reading the buffered error,
# so a single attempt occasionally yields no output. Retry until it appears; a
# genuine breakage still fails the diff because every attempt stays empty.
function expect_rejection() {
    local expected="$1"
    local payload="$2"
    local i result
    for i in {1..30}; do
        result=$(printf "$payload" | nc "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_TCP_WITH_PROXY}" | head -c250 | grep --text -o -F "$expected")
        if [ -n "$result" ]; then
            echo "$result"
            return 0
        fi
        sleep 0.3
    done
    return 1
}

expect_rejection 'client_name does not match' "PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n\0\21ClickHouse client\24\r\253\251\3\0\7default\0\4\1\0\1\0\0\t0.0.0.0:0\1\6hacker\16hacker-desktop\15Hacker client\24\r\253\251\3\0\1\0\0\0\2\1\25SELECT 'Hello, world'\2\0\247\203\254l\325\\z|\265\254F\275\333\206\342\24\202\24\0\0\0\n\0\0\0\240\1\0\2\377\377\377\377\0\0\0"
expect_rejection 'version does not match' "PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n\0\21ClickHouse client\24\r\253\251\3\0\7default\0\4\1\0\1\0\0\t0.0.0.0:0\1\6hacker\16hacker-desktop\21ClickHouse client\20\r\253\251\3\0\1\0\0\0\2\1\25SELECT 'Hello, world'\2\0\247\203\254l\325\\z|\265\254F\275\333\206\342\24\202\24\0\0\0\n\0\0\0\240\1\0\2\377\377\377\377\0\0\0"
