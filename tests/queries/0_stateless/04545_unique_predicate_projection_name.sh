#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The projection (column) name of the UNIQUE predicate must be stable: identical between the
# analyze-time header (only_analyze mode, e.g. `DESCRIBE` or distributed shard header construction)
# and the execution-time header, and independent of the boolean result. Before the fix the name was
# the folded constant's value ("1" for a unique subquery, "0" otherwise), so the analyze-time header
# (which folds to a placeholder value in only_analyze mode) disagreed with the execution-time header
# and a distributed query failed with "Block structure mismatch in ... stream: different columns".
# See https://github.com/ClickHouse/ClickHouse/pull/99877

# The name built by only_analyze (here via DESCRIBE) must equal the name built by execution.
# Capture the full output first, then take the first line, so the client is never killed by a
# broken pipe (which the test harness would report as a failure).
analyze_out=$($CLICKHOUSE_CLIENT --enable_analyzer=1 -q \
    "DESCRIBE (SELECT UNIQUE((SELECT number FROM numbers(3))))")
analyze_name=$(echo "$analyze_out" | head -n 1 | cut -f 1)
execute_out=$($CLICKHOUSE_CLIENT --enable_analyzer=1 -q \
    "SELECT UNIQUE((SELECT number FROM numbers(3))) FORMAT TSVWithNames")
execute_name=$(echo "$execute_out" | head -n 1)
[ "$analyze_name" = "$execute_name" ] \
    && echo "analyze and execute header names match" \
    || echo "MISMATCH: analyze='$analyze_name' execute='$execute_name'"

# The name must not depend on whether the subquery is actually unique.
dup_out=$($CLICKHOUSE_CLIENT --enable_analyzer=1 -q \
    "SELECT UNIQUE((SELECT number % 2 FROM numbers(4))) FORMAT TSVWithNames")
name_dup=$(echo "$dup_out" | head -n 1)
[ "$execute_name" = "$name_dup" ] \
    && echo "header name is independent of the boolean result" \
    || echo "VALUE-DEPENDENT: unique='$execute_name' dup='$name_dup'"

# Distributed sanity: UNIQUE over a remote source must not fail with a header/block-structure
# mismatch when the initiator builds the header via only_analyze and the shards execute the query.
$CLICKHOUSE_CLIENT --enable_analyzer=1 --prefer_localhost_replica=0 -q \
    "SELECT UNIQUE((SELECT number FROM numbers(3))) FROM remote('127.0.0.{1,2}', system.one) ORDER BY ALL"
