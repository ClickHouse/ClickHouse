# Shared by sqlancer_job.sh and sqlancer_pp_job.sh: scan a fuzzing run's server
# logs for a sanitizer report or a `<Fatal>` message. Both jobs run against an
# ASan+UBSan build, where either is a finding on its own even when no oracle
# noticed anything.
#
# Usage: scan_server_errors <server stdout log> <server stderr log> <report path>
# Writes the report file and echoes its first offending line; echoes nothing and
# returns 1 when the logs are clean.
#
#        server_error_signature <report path>
# Echoes a stable identity for that report: the kind of failure plus the frame it
# happened in, with addresses, pids, sizes and line numbers normalized away. Used
# as the report row name, which is also the CI DB test name and the alert
# fingerprint - so it has to survive a rerun unchanged and still tell two
# different bugs apart.
#
# Call it BEFORE stopping the server, so a leak report emitted during shutdown
# does not turn every run red.

SANITIZER_PATTERN='(ERROR|SUMMARY): (Address|Leak|Memory|Thread|UndefinedBehavior)Sanitizer|runtime error:'

scan_server_errors() {
    local server_log="$1" server_err="$2" report="$3"
    local sanitizer_hits="" fatal_hits first_hit

    # Report from the first sanitizer line to the end of the log so the whole
    # stack trace comes along, minus ASan's own boilerplate (same ignore list as
    # `ci/jobs/scripts/clickhouse_proc.py`).
    first_hit="$(grep -n -aE "$SANITIZER_PATTERN" "$server_err" 2>/dev/null | head -1 | cut -d: -f1 || true)"
    if [ -n "$first_hit" ]; then
        sanitizer_hits="$(sed -n "${first_hit},\$p" "$server_err" \
            | grep -av "ASan doesn't fully support makecontext/swapcontext functions" \
            | grep -av "ASan is ignoring requested __asan_handle_no_return" \
            | grep -av "False positive error reports may follow" \
            | grep -av "For details see https://github.com/google/sanitizers" \
            | head -n 200 || true)"
    fi
    # -h: with two files grep prefixes every match with the file name, which would
    # end up inside the report row name and the alert fingerprint - making the same
    # fatal re-hash when it moves between stdout and stderr or the workspace path
    # changes.
    fatal_hits="$(grep -ha '<Fatal>' "$server_log" "$server_err" 2>/dev/null | head -n 50 || true)"
    [ -n "$sanitizer_hits$fatal_hits" ] || return 1

    : > "$report"
    if [ -n "$sanitizer_hits" ]; then
        printf '%s\n' "=== sanitizer report ===" "$sanitizer_hits" >> "$report"
    fi
    if [ -n "$fatal_hits" ]; then
        printf '%s\n' "=== <Fatal> messages ===" "$fatal_hits" >> "$report"
    fi
    sed -n '2p' "$report" | cut -c1-400
}

server_error_signature() {
    local report="$1" kind frame
    # Line 1 is the section header written by scan_server_errors; line 2 is the
    # sanitizer summary / first `<Fatal>` line.
    kind="$(sed -n '2p' "$report" | sed -e 's/0x[0-9a-fA-F]*/ADDR/g' -e 's/[0-9][0-9]*/N/g' | cut -c1-120)"
    # The kind alone is not an identity: two unrelated heap-buffer-overflows share
    # it. Add the innermost symbolized frame - "#0 0x... in DB::Foo::bar() src/x.cpp:12".
    frame="$(grep -m1 -aE '^[[:space:]]*#0[[:space:]]' "$report" \
        | sed -e 's/.*[[:space:]]in[[:space:]]//' -e 's/0x[0-9a-fA-F]*/ADDR/g' -e 's/:[0-9]\+/:N/g' \
        | cut -c1-120 || true)"
    if [ -z "$frame" ]; then
        # UBSan reports have no frame list; their `file.cpp:line:col: runtime error`
        # prefix already went into `kind`, so only look past it for extra context.
        frame="$(sed -n '3,8p' "$report" | grep -m1 -aE '\.(cpp|h|hpp):[0-9]+' \
            | sed -e 's/0x[0-9a-fA-F]*/ADDR/g' -e 's/:[0-9]\+/:N/g' | cut -c1-120 || true)"
    fi
    printf '%s' "$kind${frame:+ @ $frame}"
}
