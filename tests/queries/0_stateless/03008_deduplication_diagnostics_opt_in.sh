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
        # One row per statement: join lines, split on the statement terminator, collapse
        # whitespace so the dump is insensitive to the generator's indentation only.
        printf '%s\n' "$emit" | tr '\n' ' ' | tr ';' '\n' \
            | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//' \
            | grep -v '^$' \
            | sed "s|^|$sub $phase stmt |"
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
for drv in "$CURDIR"/03008_deduplication_*_replicated.sh "$CURDIR"/03008_deduplication_*_nonreplicated.sh; do
    name=$(basename "$drv" .sh)
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
          echo 'CASE_ARGS=(--insert-method InsertSelect --table-engine MergeTree --use-insert-token True
            --single-thread True --deduplicate-src-table True --deduplicate-dst-table True
            --insert-unique-blocks True --get-logs false)'
          sed 's/^                        //' "$W/block.sh"
        } > "$W/run.sh"
        out=$(bash "$W/run.sh" 2> "$W/fwd")
        echo "$name $phase failure verdict [$out]"
        echo "$name $phase failure client invocations $(cat "$W/n")"
        echo "$name $phase failure rerun is diagnostics-only $( [ -f "$W/sql.2" ] && { grep -c 'DEBUG' "$W/sql.2" || true; } | awk '{print ($1>0)?1:0}' )"
        echo "$name $phase failure rerun mutating $( [ -f "$W/sql.2" ] && { grep -ciE 'DROP |CREATE |INSERT |throwIf' "$W/sql.2" || true; } || echo NA )"
        # The rerun must carry THIS phase's diagnostics, not the other phase's.
        echo "$name $phase failure rerun phase $( [ -f "$W/sql.2" ] && { grep -c "DEBUG $phase" "$W/sql.2" || true; } )"
        echo "$name $phase failure marker forwarded $(grep -c 'DEDUP_ASSERT_FAILED' "$W/fwd")"
        rm -rf "$W"
    done
done
