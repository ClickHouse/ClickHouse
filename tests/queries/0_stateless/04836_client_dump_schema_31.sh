#!/usr/bin/env bash
# Tags: no-fasttest

# --dump-schema picks the typing settings under the analyzer gates the replay has, and sets a gate the chosen settings need.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# --dump-schema refuses input on stdin, whatever the test runner passes.
exec < /dev/null

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT
cd "$work" || exit 1

# Creates `$3`, dumps database d with the session settings `$2` and replays the dump with the defaults.
replay()
{
    local name=$1 settings=$2 sql=$3
    $CLICKHOUSE_LOCAL --path "$work/$name/src" -n -q "CREATE DATABASE d; $sql" || return
    # shellcheck disable=SC2086
    $CLICKHOUSE_LOCAL --path "$work/$name/src" $settings --dump-schema d > "$work/$name/dump.sql" 2> "$work/$name/err.txt" \
        || { echo "$name: dump failed: $(cat "$work/$name/err.txt")"; return; }
    grep -h 'typing\|supertype' "$work/$name/err.txt"
    grep '^SET allow_lossy' "$work/$name/dump.sql"
    echo "$name: GROUP BY gate lines: $(grep -c '^SET allow_suspicious_types_in_group_by = 1;' "$work/$name/dump.sql")"
    $CLICKHOUSE_LOCAL --path "$work/$name/dst" -n --queries-file "$work/$name/dump.sql" > "$work/$name/out.txt" 2>&1
    echo "$name: replay rc=$? $(head -c 200 "$work/$name/out.txt")"
}

# Its key is Float64 with the setting on and a Variant with it off, which GROUP BY refuses without the gate.
replay mv_group_by "--allow_lossy_numeric_supertype 1" "SET allow_lossy_numeric_supertype = 1;
    CREATE TABLE d.src (a UInt64, c UInt8) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW d.mv ENGINE = MergeTree ORDER BY k
    AS SELECT if(c > 0, toDecimal64(a, 2), 0.5) AS k, count() AS n FROM d.src GROUP BY k;"
# A Variant key needs the gate under every combination, so it is also set right before the CREATE.
replay mv_variant_key "" "SET allow_suspicious_types_in_group_by = 1;
    CREATE TABLE d.src (a UInt64, v Variant(UInt64, String)) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW d.mv ENGINE = Memory AS SELECT v, if(a > 0, 1, 2) AS b, count() AS n FROM d.src GROUP BY v, b;"
