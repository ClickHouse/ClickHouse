#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Runs a query that must fail and reports whether the given text is in the error. The client echoes
# the server log as well, so the text can occur more than once; only its presence is asserted.
expect_in_error()
{
    local pattern="$1"
    local query="$2"
    if $CLICKHOUSE_CLIENT -q "$query" 2>&1 | grep -qF "$pattern"; then
        echo "found: $pattern"
    else
        echo "missing: $pattern"
    fi
}

# `SETTINGS` as the only argument of the table function: the structure stays "auto".
$CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT * FROM generateRandom(SETTINGS null_ratio = 0.1) LIMIT 10)"

# `SETTINGS` as the only argument, with the structure taken from the insertion table.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS settings_hint;
    CREATE TABLE settings_hint (a Nullable(UInt32), b String) ENGINE = Memory;
    INSERT INTO settings_hint SELECT * FROM generateRandom(SETTINGS null_ratio = 0.1) LIMIT 10;
    SELECT count() FROM settings_hint;
    DROP TABLE settings_hint;
"

# An unknown setting is rejected on the table function and on the engine.
expect_in_error "UNKNOWN_SETTING" "SELECT * FROM generateRandom('x UInt8', 1, SETTINGS no_such_setting = 1) LIMIT 1"
expect_in_error "UNKNOWN_SETTING" "CREATE TABLE settings_unknown (x UInt8) ENGINE = GenerateRandom(1) SETTINGS no_such_setting = 1"

# Values outside the allowed range are rejected, and the error names the setting and its bound.
expect_in_error 'Setting `null_ratio` must be in [0, 1], got 1.5' "SELECT * FROM generateRandom('x UInt8', 1, SETTINGS null_ratio = 1.5) LIMIT 1"
expect_in_error 'Setting `null_ratio` must be in [0, 1], got -0.1' "SELECT * FROM generateRandom('x UInt8', 1, SETTINGS null_ratio = -0.1) LIMIT 1"
expect_in_error 'Setting `max_json_depth` must be in [1, 32], got 0' "SELECT * FROM generateRandom('x UInt8', 1, SETTINGS max_json_depth = 0) LIMIT 1"
expect_in_error 'Setting `max_json_depth` must be in [1, 32], got 33' "SELECT * FROM generateRandom('x UInt8', 1, SETTINGS max_json_depth = 33) LIMIT 1"
expect_in_error 'Setting `max_json_keys_per_object` must be at most 1000, got 1001' "SELECT * FROM generateRandom('x UInt8', 1, SETTINGS max_json_keys_per_object = 1001) LIMIT 1"
expect_in_error 'Setting `max_json_depth` must be in [1, 32], got 33' "CREATE TABLE settings_bad (x UInt8) ENGINE = GenerateRandom(1) SETTINGS max_json_depth = 33"

# The types the documentation lists as unsupported are refused with a clear message.
expect_in_error "The 'GenerateRandom' is not implemented for type AggregateFunction(sum, UInt64)" "SELECT * FROM generateRandom('x AggregateFunction(sum, UInt64)') LIMIT 1"
expect_in_error "The 'GenerateRandom' is not implemented for type IntervalSecond" "SELECT * FROM generateRandom('x IntervalSecond') LIMIT 1"
expect_in_error "The 'GenerateRandom' is not implemented for type QBit(BFloat16, 16)" "SELECT * FROM generateRandom('x QBit(BFloat16, 16)') LIMIT 1"

# Engine settings survive a `DETACH`/`ATTACH` round trip.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS settings_attach;
    CREATE TABLE settings_attach (x Nullable(UInt8)) ENGINE = GenerateRandom(1) SETTINGS null_ratio = 0.5;
    DETACH TABLE settings_attach;
    ATTACH TABLE settings_attach;
"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE settings_attach" | grep -c "null_ratio = 0.5"
$CLICKHOUSE_CLIENT -q "DROP TABLE settings_attach"

# The `SETTINGS` argument is taken out of the argument list in place, so it has to survive both a
# `DESCRIBE` of the call and a view that is analyzed again on every read.
$CLICKHOUSE_CLIENT -q "DESCRIBE generateRandom('x Nullable(UInt8)', 1, SETTINGS null_ratio = 1)"
$CLICKHOUSE_CLIENT -q "
    DROP VIEW IF EXISTS settings_view;
    CREATE VIEW settings_view AS SELECT * FROM generateRandom('x Nullable(UInt8)', 1, SETTINGS null_ratio = 1) LIMIT 10;
"
$CLICKHOUSE_CLIENT -q "SHOW CREATE VIEW settings_view" | grep -c "null_ratio = 1"
$CLICKHOUSE_CLIENT -q "SELECT countIf(x IS NULL) FROM settings_view"
$CLICKHOUSE_CLIENT -q "SELECT countIf(x IS NULL) FROM settings_view"
$CLICKHOUSE_CLIENT -q "DROP VIEW settings_view"

# The fourth positional argument is `max_array_length`, and the error message says so.
expect_in_error "max_array_length" "SELECT * FROM generateRandom('x UInt8', 1, 10, 'abc') LIMIT 1"
expect_in_error "max_string_length" "SELECT * FROM generateRandom('x UInt8', 1, 10, 'abc') LIMIT 1"
