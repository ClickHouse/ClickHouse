#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t"

$CLICKHOUSE_CLIENT --allow_deprecated_syntax_for_merge_tree=1 --query="CREATE TABLE t (CounterID UInt32, StartDate Date, UserID UInt32, VisitID UInt32, NestedColumn Nested(A UInt8, S String), ToDrop UInt32) ENGINE = MergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192)"

$CLICKHOUSE_CLIENT --query="ALTER TABLE t DROP COLUMN ToDro" 2>&1 | grep -q "Maybe you meant: \['ToDrop'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="ALTER TABLE t MODIFY COLUMN ToDro UInt64" 2>&1 | grep -q "Maybe you meant: \['ToDrop'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="ALTER TABLE t RENAME COLUMN ToDro to ToDropp" 2>&1 | grep -q "Maybe you meant: \['ToDrop'\]" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT --query="DROP TABLE t"
