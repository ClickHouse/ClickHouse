#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A qualified COLUMNS matcher must survive a format round-trip inside an INSERT column list.
# Formatting `q.* [I]LIKE 'pattern'` yields the canonical `q.COLUMNS('regexp')` form, which the
# INSERT column-list parser previously rejected, so the second parse failed with a syntax error
# (STID 1941-26fa). Piping through the formatter twice must reproduce the exact same text.

roundtrip() {
    $CLICKHOUSE_FORMAT --oneline --query "$1" | $CLICKHOUSE_FORMAT --oneline
}

roundtrip "INSERT INTO t (a.* ILIKE '%a%') SELECT 1"
roundtrip "INSERT INTO t (a.COLUMNS('(?i)a')) SELECT 1"
roundtrip "INSERT INTO t (a.* LIKE '%a%') SELECT 1"
roundtrip "INSERT INTO t (a.COLUMNS('b')) SELECT 1"
roundtrip "INSERT INTO t (a.COLUMNS(x, y)) SELECT 1"
roundtrip "INSERT INTO t (\`numbers(4)\`.* ILIKE '%a%', m.value, m.total) FORMAT Values"
roundtrip "INSERT INTO t (x.COLUMNS('re') APPLY toString EXCEPT (a, b) REPLACE (5 AS z)) SELECT 1"

# Regular column-list forms must keep working.
roundtrip "INSERT INTO t (a, b, c) SELECT 1, 2, 3"
roundtrip "INSERT INTO t (a.b, c.d) SELECT 1, 2"
roundtrip "INSERT INTO t (COLUMNS('a')) SELECT 1"
roundtrip "INSERT INTO t (a.*) SELECT 1"
