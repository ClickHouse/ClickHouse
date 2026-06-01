#!/usr/bin/env bash
# Tags: long
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/92718
# Verifies that `--chime N` makes the client emit ASCII `BEL` (`\x07`) on stderr
# when a query finishes after running for at least N seconds AND stderr is
# attached to a terminal. When stderr is redirected to a file or pipe (the
# common case for automation, including `clickhouse-test`), `BEL` is suppressed
# so the captured stderr stream stays clean. Works in both success and error
# paths and applies to clickhouse-client and clickhouse-local.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

bel_count() {
    # Print "BEL" if input file contains ASCII BEL (`\x07`), "no BEL" otherwise.
    if grep -q $'\x07' "$1"; then
        echo "BEL"
    else
        echo "no BEL"
    fi
}

err1="${CLICKHOUSE_TMP}/04303_err1_${CLICKHOUSE_DATABASE}.txt"
err2="${CLICKHOUSE_TMP}/04303_err2_${CLICKHOUSE_DATABASE}.txt"
err3="${CLICKHOUSE_TMP}/04303_err3_${CLICKHOUSE_DATABASE}.txt"
err4="${CLICKHOUSE_TMP}/04303_err4_${CLICKHOUSE_DATABASE}.txt"
err5="${CLICKHOUSE_TMP}/04303_err5_${CLICKHOUSE_DATABASE}.txt"
err6="${CLICKHOUSE_TMP}/04303_err6_${CLICKHOUSE_DATABASE}.txt"
tty7="${CLICKHOUSE_TMP}/04303_tty7_${CLICKHOUSE_DATABASE}.txt"
tty8="${CLICKHOUSE_TMP}/04303_tty8_${CLICKHOUSE_DATABASE}.txt"
tty9="${CLICKHOUSE_TMP}/04303_tty9_${CLICKHOUSE_DATABASE}.txt"
tty10="${CLICKHOUSE_TMP}/04303_tty10_${CLICKHOUSE_DATABASE}.txt"
tty11="${CLICKHOUSE_TMP}/04303_tty11_${CLICKHOUSE_DATABASE}.txt"

# -----------------------------------------------------------------------------
# Non-TTY cases (stderr redirected to a regular file). With the TTY guard, the
# chime is suppressed in all of these — that is the new contract. Each case
# also exercises the threshold logic so regressing the threshold would still
# show up via case ordering.
# -----------------------------------------------------------------------------

# Case 1: `--chime 1`, slow query, stderr redirected — expect no `BEL`
# (suppressed because stderr is not a TTY).
${CLICKHOUSE_CLIENT} --chime 1 -q "SELECT sleep(1.5) FORMAT Null" 2> "$err1" > /dev/null
echo "1. clickhouse-client --chime 1, slow query, redirected stderr: $(bel_count "$err1")"

# Case 2: `--chime 10`, fast query — expect no `BEL` (below threshold).
${CLICKHOUSE_CLIENT} --chime 10 -q "SELECT sleep(0.1) FORMAT Null" 2> "$err2" > /dev/null
echo "2. clickhouse-client --chime 10, fast query: $(bel_count "$err2")"

# Case 3: no `--chime` flag, query below the default 5-second threshold — expect no `BEL`.
${CLICKHOUSE_CLIENT} -q "SELECT sleep(1.5) FORMAT Null" 2> "$err3" > /dev/null
echo "3. clickhouse-client (no --chime), 1.5s < default 5s threshold: $(bel_count "$err3")"

# Case 4: `--chime 1`, slow query that ends in an error, stderr redirected — expect no
# `BEL` (suppressed by the TTY guard) AND verify the error path actually fires so the
# test would loudly notice if `throwIf` semantics or the error path itself changes.
${CLICKHOUSE_CLIENT} --chime 1 -q "SELECT sleep(1.5), throwIf(1 = 1, 'expected error')" 2> "$err4" > /dev/null
rc=$?
if [ "$rc" -eq 0 ]; then
    case4_status="FAIL: query unexpectedly succeeded"
elif ! grep -q 'expected error' "$err4"; then
    case4_status="FAIL: missing 'expected error' message in stderr"
else
    case4_status=$(bel_count "$err4")
fi
echo "4. clickhouse-client --chime 1, slow query then error, redirected stderr: $case4_status"

# Case 5: same suppression must hold for `clickhouse-local` so the feature is wired in
# `ClientBase` consistently across binaries.
${CLICKHOUSE_LOCAL} --chime 1 -q "SELECT sleep(1.5) FORMAT Null" 2> "$err5" > /dev/null
echo "5. clickhouse-local --chime 1, slow query, redirected stderr: $(bel_count "$err5")"

# Case 6: `--chime 0` explicitly disables the chime — expect no `BEL` even in contexts
# that would otherwise emit it.
${CLICKHOUSE_CLIENT} --chime 0 -q "SELECT sleep(1.5) FORMAT Null" 2> "$err6" > /dev/null
echo "6. clickhouse-client --chime 0, slow query: $(bel_count "$err6")"

# -----------------------------------------------------------------------------
# TTY cases — we run the client under `script -qc` so that its stderr is attached
# to a pseudo-terminal allocated by `script`. The pty output is captured to a file
# (combined with stdout) which we then grep for `BEL`. `FORMAT Null` keeps stdout
# empty so only chime-related bytes appear in the captured stream.
# -----------------------------------------------------------------------------

# Case 7: `--chime 1`, slow query, stderr attached to a pty — expect `BEL`.
/usr/bin/script -qc "${CLICKHOUSE_CLIENT} --chime 1 -q 'SELECT sleep(1.5) FORMAT Null'" /dev/null > "$tty7" 2>&1
echo "7. clickhouse-client --chime 1, slow query, pty stderr: $(bel_count "$tty7")"

# Case 8: `--chime 0` over a pty, slow query — expect no `BEL` (explicit disable wins
# over the default-on behaviour even on a TTY).
/usr/bin/script -qc "${CLICKHOUSE_CLIENT} --chime 0 -q 'SELECT sleep(1.5) FORMAT Null'" /dev/null > "$tty8" 2>&1
echo "8. clickhouse-client --chime 0, slow query, pty stderr: $(bel_count "$tty8")"

# Case 9: `--chime 10`, fast query, stderr attached to a pty — expect no `BEL`.
# This is the only path where `BEL` is emitted, so it is the one place where a
# regression of the threshold gate (`elapsedSeconds() >= chime_threshold_seconds`)
# would actually surface. Cases 1-6 cannot catch such a regression because the
# TTY guard suppresses `BEL` regardless of the threshold.
/usr/bin/script -qc "${CLICKHOUSE_CLIENT} --chime 10 -q 'SELECT sleep(0.1) FORMAT Null'" /dev/null > "$tty9" 2>&1
echo "9. clickhouse-client --chime 10, fast query (0.1s < 10s threshold), pty stderr: $(bel_count "$tty9")"

# Case 10: no `--chime` flag, slow query (~6s > default 5s threshold), stderr
# attached to a pty — expect `BEL`. This verifies the advertised default-on
# behaviour: when `--chime` is omitted the client must still emit `BEL` once a
# query exceeds the default 5-second threshold. Cases 3 and 5 only cover the
# non-TTY suppression branch and would silently pass if `implicit_value` /
# `default_value` regressed to `0` (i.e. accidentally disabled by default).
/usr/bin/script -qc "${CLICKHOUSE_CLIENT} -q 'SELECT sleepEachRow(2) FROM numbers(3) FORMAT Null SETTINGS function_sleep_max_microseconds_per_block = 0'" /dev/null > "$tty10" 2>&1
echo "10. clickhouse-client (no --chime), slow query (~6s > default 5s threshold), pty stderr: $(bel_count "$tty10")"

# Case 11: `--chime 1`, slow query that ends in an error, stderr attached to a
# pty — expect `BEL` on the error path AND verify the query actually failed
# (non-zero exit + `'expected error'` in the captured stream). This is the only
# place where the "chime on error in TTY" contract is exercised: case 4 covers
# the non-TTY suppression branch and would still pass if a regression emitted
# `BEL` only on success and silently dropped it on errors.
/usr/bin/script -eqc "${CLICKHOUSE_CLIENT} --chime 1 -q \"SELECT sleep(1.5), throwIf(1 = 1, 'expected error')\"" /dev/null > "$tty11" 2>&1
rc=$?
if [ "$rc" -eq 0 ]; then
    case11_status="FAIL: query unexpectedly succeeded"
elif ! grep -q 'expected error' "$tty11"; then
    case11_status="FAIL: missing 'expected error' message in pty output"
else
    case11_status=$(bel_count "$tty11")
fi
echo "11. clickhouse-client --chime 1, slow query then error, pty stderr: $case11_status"

rm -f "$err1" "$err2" "$err3" "$err4" "$err5" "$err6" "$tty7" "$tty8" "$tty9" "$tty10" "$tty11"
