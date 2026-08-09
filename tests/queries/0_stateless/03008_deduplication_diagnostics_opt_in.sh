#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

GEN="$CURDIR/03008_deduplication.python"
ARGS=(--insert-method InsertSelect --table-engine MergeTree --use-insert-token True
      --single-thread True --deduplicate-src-table True --deduplicate-dst-table True
      --insert-unique-blocks True --get-logs false)
# A second shape. Its two phases have different expected counts, so a corrupted phase
# expectation cannot hide behind the other phase's, and single_thread=False reaches the
# max_insert_threads branch of the settings report.
ARGS_MIXED=(--insert-method InsertSelect --table-engine MergeTree --use-insert-token True
      --single-thread False --deduplicate-src-table False --deduplicate-dst-table False
      --insert-unique-blocks True --get-logs false)

# Each alternative carries its own terminating semicolon so a longer value cannot match as a
# prefix: without it, "SET max_insert_threads=10;" matches the "max_insert_threads=1" branch
# and a diagnostics run reporting a value the failing run never used would pass unnoticed.
# Only the ARGS shape needs a pattern; the ARGS_MIXED shape's values are pinned literally by
# the statement dump below.
SETTINGS_RE_ST='SET max_insert_threads=1;|SET update_insert_deduplication_token_in_dependent_materialized_views=1;|SET deduplicate_blocks_in_dependent_materialized_views=1;|SET max_block_size=1;'

# One row per statement: join lines, split on the statement terminator, collapse whitespace so
# the result is insensitive to the generator's indentation only. Shared by the statement dump
# and by the driver replay's payload-equality check so the two cannot normalize differently.
norm() {
    tr '\n' ' ' | tr ';' '\n' \
        | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//' \
        | grep -v '^$'
}

for sub in insert_several_blocks_into_table mv_generates_several_blocks several_mv_into_one_table; do
    main=$(python3 "$GEN" "$sub" "${ARGS[@]}")
    # The success path carries no diagnostics at all.
    echo "$sub main DEBUG $(echo "$main" | grep -c 'DEBUG')"
    # Every assertion names its phase in the exact spelling the drivers parse,
    # so a failing case can always be attributed to one phase.
    echo "$sub main markers first $(echo "$main" | grep -c 'DEDUP_ASSERT_FAILED phase=first ')"
    echo "$sub main markers second $(echo "$main" | grep -c 'DEDUP_ASSERT_FAILED phase=second ')"

    for phase in first second; do
        emit=$(python3 "$GEN" "$sub" "${ARGS[@]}" --emit-debug-only "$phase")
        # The whole diagnostics block is carried over, not just the basic probes ...
        echo "$sub $phase emit DEBUG total $(echo "$emit" | grep -c 'DEBUG')"
        echo "$sub $phase emit DEBUG phase $(echo "$emit" | grep -c "DEBUG $phase")"
        # ... the settings the diagnostics report on are applied with the failing run's
        # values, not just some SET lines ...
        echo "$sub $phase emit probed settings $(echo "$emit" | grep -oE "$SETTINGS_RE_ST" | sort -u | wc -l)"
        # ... and it cannot destroy or re-create the state it is inspecting.
        echo "$sub $phase emit mutating $(echo "$emit" | grep -ciE 'DROP |CREATE |INSERT |throwIf')"
    done
done

# The payload of the diagnostics, statement by statement, not just aggregate counts of it.
# Enumerating individual observables (projections, FROM tables, guard constants, ...) can only
# pin the clauses somebody thought to enumerate: a changed WHERE filter, IN list or ORDER BY
# stayed invisible. Dumping every statement normalized is closed under that class by
# construction, and a diff names the exact statements that moved. It also subsumes the
# aggregate rows it replaces: the guard constants, read tables, projections, the applied
# SET values and the absence of DROP/CREATE/INSERT/throwIf are all literally in the dump.
for sub in insert_several_blocks_into_table mv_generates_several_blocks several_mv_into_one_table; do
    for phase in first second; do
        emit=$(python3 "$GEN" "$sub" "${ARGS_MIXED[@]}" --emit-debug-only "$phase")
        printf '%s\n' "$emit" | norm | sed "s|^|$sub $phase stmt |"
        # On master the probes ran on the success path, so invalid SQL in them broke every
        # case immediately. They only run on failure now, so nothing else would notice. Kept
        # alongside the dump because it fails with a clearer signal than a multi-line diff.
        if printf '%s' "$emit" | $CLICKHOUSE_FORMAT --multiquery > /dev/null 2>&1; then
            echo "$sub $phase emit parses 1"
        else
            echo "$sub $phase emit parses 0"
        fi
    done
done

# A marker the generator emits must be one every driver can parse, and each driver must
# still take the main invocation without diagnostics and re-run them for the failed phase.
# Both phases are checked: a driver whose parser recognized only one of them would leave the
# other phase's failure with an empty FAILED_PHASE, so the guarded rerun is skipped and that
# failure ships with no diagnostics at all.
MAIN_OUT=$(python3 "$GEN" several_mv_into_one_table "${ARGS[@]}")
for drv in "$CURDIR"/03008_deduplication_*_replicated.sh "$CURDIR"/03008_deduplication_*_nonreplicated.sh; do
    name=$(basename "$drv" .sh)
    prog=$(sed -n "s/.*FAILED_PHASE=\$(sed -n '\(.*\)' \"\\\$CASE_STDERR\".*/\1/p" "$drv")
    for phase in first second; do
        # The line is built from a marker the generator really emits for this phase, so the row
        # also fails if the generator stops emitting it in the shape the drivers parse.
        MARKER=$(printf '%s\n' "$MAIN_OUT" \
            | grep -oE "DEDUP_ASSERT_FAILED phase=$phase table=[a-z_]+" | head -n 1)
        LINE="Code: 395. DB::Exception: ${MARKER}: while executing"
        echo "$name $phase parses [$(echo "$LINE" | sed -n "$prog" | head -n 1)]"
    done
    # Per-driver, not per-phase, so these stay outside the phase loop.
    echo "$name main debug-on-fail $(grep -c 'debug-on-fail' "$drv")"
    # shellcheck disable=SC2016  # the literal text $FAILED_PHASE is searched for, not expanded
    echo "$name emit-debug-only $(grep -c 'emit-debug-only "\$FAILED_PHASE"' "$drv")"
done

# Drive each driver's own failure-handling block, extracted verbatim from the driver, with a
# stub client that fails the first invocation the way the server does. That makes the guard,
# the diagnostics rerun and the stderr forwarding observable instead of merely present.
# Both phases are driven, because a driver that dispatched only one of them would silently
# skip the rerun for the other and the failure would carry no diagnostics.
#
# The rerun is checked by comparing its whole payload against what the generator produces for
# this driver's own subcommand and this replay's own arguments. Asserting properties of the
# rerun instead (it contains DEBUG, it names this phase, it mutates nothing) can only pin the
# properties somebody thought of: dropping "${CASE_ARGS[@]}" or swapping the subcommand leaves
# every such property unchanged while being a real defect in the driver.
#
# This shape must DIFFER from the generator's argparse defaults, otherwise a rerun that dropped
# "${CASE_ARGS[@]}" would still produce a byte-identical payload and the check would be vacuous.
# Measured: against the defaults it moves SET max_insert_threads 1 -> 10 and every destination
# guard constant (26 statements for phase first, 30 for second). Do not "simplify" it back to
# the defaults.
REPLAY_ARGS=(--insert-method InsertSelect --table-engine MergeTree --use-insert-token False
      --single-thread False --deduplicate-src-table False --deduplicate-dst-table False
      --insert-unique-blocks False --get-logs false)
for drv in "$CURDIR"/03008_deduplication_*_replicated.sh "$CURDIR"/03008_deduplication_*_nonreplicated.sh; do
    name=$(basename "$drv" .sh)
    # Derived from the driver's MAIN invocation, not from its rerun line: reading it from the
    # rerun would make the payload comparison tautological under the subcommand-swap mutation
    # it exists to catch. Emitted in a row of its own so a driver edit that breaks the
    # derivation fails loudly instead of silently comparing against an empty subcommand.
    # shellcheck disable=SC2016  # the literal text ${CASE_ARGS[@]} is searched for, not expanded
    SUB=$(sed -n 's|.*03008_deduplication\.python \([a-z_]*\) "\${CASE_ARGS\[@\]}")$|\1|p' "$drv" | head -n 1)
    echo "$name main subcommand [$SUB]"
    for phase in first second; do
        W=$(mktemp -d "$CLICKHOUSE_TMP/03008_dedup_probe_XXXXXX")
        # Expanding here-doc: only $phase is substituted, the stub's own variables are escaped.
        cat > "$W/client" <<STUB
#!/usr/bin/env bash
D="\$(dirname "\$0")"
n=\$(cat "\$D/n" 2>/dev/null || echo 0); n=\$((n+1)); echo "\$n" > "\$D/n"
printf '%s' "\${!#}" > "\$D/sql.\$n"
if [ "\$n" = 1 ]; then
  echo "Code: 395. DB::Exception: DEDUP_ASSERT_FAILED phase=$phase table=t: while executing" >&2
  exit 395
fi
exit 0
STUB
        chmod +x "$W/client"
        # shellcheck disable=SC2016  # the literal text $CLICKHOUSE_CLIENT is searched for, not expanded
        sed -n '/^                        if \$CLICKHOUSE_CLIENT/,/^                        fi$/p' "$drv" > "$W/block.sh"
        {
          echo "CLICKHOUSE_CLIENT='$W/client'"
          echo "CASE_STDERR='$W/stderr'"
          echo "CURDIR='$CURDIR'"
          # The same array the payload comparison below uses, written once and interpolated, so
          # the driver's rerun and the expected payload cannot be computed from two shapes that
          # silently drift apart and make the comparison vacuous.
          echo "CASE_ARGS=(${REPLAY_ARGS[*]})"
          sed 's/^                        //' "$W/block.sh"
        } > "$W/run.sh"
        out=$(bash "$W/run.sh" 2> "$W/fwd")
        echo "$name $phase failure verdict [$out]"
        echo "$name $phase failure client invocations $(cat "$W/n")"
        # The rerun's whole payload, not just properties of it: this pins WHICH subcommand and
        # WHICH arguments the driver replayed with, so dropping the case arguments or replaying
        # a different subcommand cannot pass.
        exp=$(python3 "$GEN" "$SUB" "${REPLAY_ARGS[@]}" --emit-debug-only "$phase" | norm)
        got=$( [ -f "$W/sql.2" ] && norm < "$W/sql.2" || echo MISSING )
        echo "$name $phase failure rerun payload matches $( [ "$exp" = "$got" ] && echo 1 || echo 0 )"
        # Subsumed by the payload comparison, but kept because it fails with a far clearer
        # signal than a payload mismatch: the rerun must not destroy or re-create the state the
        # failed assertion left behind.
        echo "$name $phase failure rerun mutating $( [ -f "$W/sql.2" ] && { grep -ciE 'DROP |CREATE |INSERT |throwIf' "$W/sql.2" || true; } || echo NA )"
        echo "$name $phase failure marker forwarded $(grep -c 'DEDUP_ASSERT_FAILED' "$W/fwd")"

        # The same block again, but interrupted while the diagnostics rerun is in flight. The
        # harness SIGTERMs the whole process group when the per-test deadline expires, and the
        # driver's EXIT trap then deletes CASE_STDERR, so an assertion error that has not been
        # forwarded yet is lost: the reader gets a timeout naming neither the case nor the phase.
        # Forwarding it before the rerun is what keeps that from being a regression against the
        # pre-PR behaviour, where the client wrote to the real stderr directly.
        #
        # Deterministic, not timing-dependent: the stub announces that the rerun has started and
        # waits, so the signal always lands inside the rerun. Its own directory, so the rows above
        # keep reading the returning stub's counters and payloads unchanged.
        I="$W/i"
        mkdir -p "$I"
        cat > "$I/client_block" <<STUB
#!/usr/bin/env bash
D="\$(dirname "\$0")"
n=\$(cat "\$D/n" 2>/dev/null || echo 0); n=\$((n+1)); echo "\$n" > "\$D/n"
if [ "\$n" = 1 ]; then
  echo "Code: 395. DB::Exception: DEDUP_ASSERT_FAILED phase=$phase table=t: while executing" >&2
  exit 395
fi
: > "\$D/rerun_started"
# Bounded, and released by the driver below the moment the signal has been sent: an
# unbounded wait would leave one spinning process per row behind on every run.
i=0
while [ ! -f "\$D/go" ] && [ \$i -lt 600 ]; do sleep 0.05; i=\$((i+1)); done
exit 0
STUB
        chmod +x "$I/client_block"
        {
          echo "CLICKHOUSE_CLIENT='$I/client_block'"
          # The driver's own capture and cleanup, which is what makes an unforwarded error
          # unrecoverable once the shell is signalled.
          echo "CASE_STDERR=\$(mktemp -p '$I' 03008_dedup_stderr_XXXXXX)"
          echo "trap 'rm -f \"\$CASE_STDERR\"' EXIT"
          echo "CURDIR='$CURDIR'"
          echo "CASE_ARGS=(${REPLAY_ARGS[*]})"
          sed 's/^                        //' "$W/block.sh"
        } > "$I/run.sh"
        bash "$I/run.sh" > /dev/null 2> "$I/fwd_int" &
        int_pid=$!
        i=0
        while [ ! -f "$I/rerun_started" ] && [ "$i" -lt 200 ]; do sleep 0.05; i=$((i+1)); done
        kill -TERM "$int_pid" 2>/dev/null
        : > "$I/go"
        wait "$int_pid" 2>/dev/null
        echo "$name $phase interrupted rerun forwards marker $(grep -c 'DEDUP_ASSERT_FAILED' "$I/fwd_int")"
        rm -rf "$W"
    done
done
