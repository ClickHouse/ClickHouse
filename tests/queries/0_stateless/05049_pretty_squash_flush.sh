#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pretty formats squash consecutive blocks in a background thread. The rendered table has to reach
# the client as soon as it is written, not stay in the output buffer until the query finishes.
# The query below never ends on its own: it prints two rows from the very first block and then keeps
# reading. If nothing is flushed, the file stays empty and the test fails against the reference.

output="${CLICKHOUSE_TMP}/05049_pretty_squash_flush.out"
: > "$output"

$CLICKHOUSE_LOCAL --query "SELECT DISTINCT number % 2 AS x FROM numbers(1e18) FORMAT PrettyCompact" > "$output" 2>/dev/null &
pid=$!

# Up to 30 seconds for the four lines of the table to appear.
for _ in {1..300}
do
    if [ "$(wc -l < "$output")" -ge 4 ]
    then
        break
    fi
    sleep 0.1
done

# The braces keep the shell's report about the terminated job out of stderr.
{ kill "$pid"; wait "$pid"; } 2>/dev/null

cat "$output"
rm -f "$output"
