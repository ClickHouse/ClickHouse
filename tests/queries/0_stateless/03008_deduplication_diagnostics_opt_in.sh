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
# One pattern per argument shape, since the two shapes apply different values.
SETTINGS_RE_ST='SET max_insert_threads=1;|SET update_insert_deduplication_token_in_dependent_materialized_views=1;|SET deduplicate_blocks_in_dependent_materialized_views=1;|SET max_block_size=1;'
SETTINGS_RE_MT='SET max_insert_threads=10;|SET update_insert_deduplication_token_in_dependent_materialized_views=1;|SET deduplicate_blocks_in_dependent_materialized_views=1;|SET max_block_size=1;'

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

# The payload of the diagnostics, not just how many statements it has.
for sub in insert_several_blocks_into_table mv_generates_several_blocks several_mv_into_one_table; do
    for phase in first second; do
        emit=$(python3 "$GEN" "$sub" "${ARGS_MIXED[@]}" --emit-debug-only "$phase")
        # The guard constants each phase compares against, so a phase reporting the other
        # phase's expectations shows up here.
        echo "$sub $phase guards [$(echo "$emit" | grep -oE '!= [0-9]+' | sort -u | tr '\n' ' ')]"
        # Every table the probes read, so a probe aimed at the wrong table shows up here.
        echo "$sub $phase tables [$(echo "$emit" | grep -oE 'FROM [a-z_.]+' | sort -u | sed 's/FROM //' | tr '\n' ' ')]"
        # The payload each probe actually selects. Nothing else in the file depends on the
        # projection lists, and CLICKHOUSE_FORMAT only parses the AST, so a thinned or
        # unresolvable probe would otherwise pass CI and only fail when a case really fails.
        echo "$sub $phase projections [$(echo "$emit" \
            | grep -oE 'SELECT [A-Za-z_][A-Za-z_0-9, ()]*$' \
            | sed 's/^ *//;s/ *$//' | sort -u | tr '\n' ' ')]"
        # The settings block carries the failing run's values, not fresh-session defaults.
        echo "$sub $phase threads [$(echo "$emit" | grep -oE 'SET max_insert_threads=[0-9]+')]"
        # This shape's own expected values, so a setting mutated only on the many-threads
        # branch cannot hide behind the single-thread shape's expectation.
        echo "$sub $phase emit probed settings $(echo "$emit" | grep -oE "$SETTINGS_RE_MT" | sort -u | wc -l)"
        # On master the probes ran on the success path, so invalid SQL in them broke every
        # case immediately. They only run on failure now, so nothing else would notice.
        if printf '%s' "$emit" | $CLICKHOUSE_FORMAT --multiquery > /dev/null 2>&1; then
            echo "$sub $phase emit parses 1"
        else
            echo "$sub $phase emit parses 0"
        fi
    done
done

# A marker the generator emits must be one every driver can parse, and each driver must
# still take the main invocation without diagnostics and re-run them for the failed phase.
MARKER=$(python3 "$GEN" several_mv_into_one_table "${ARGS[@]}" \
    | grep -oE 'DEDUP_ASSERT_FAILED phase=second table=[a-z_]+' | head -n 1)
LINE="Code: 395. DB::Exception: ${MARKER}: while executing"
for drv in "$CURDIR"/03008_deduplication_*_replicated.sh "$CURDIR"/03008_deduplication_*_nonreplicated.sh; do
    name=$(basename "$drv" .sh)
    prog=$(sed -n "s/.*FAILED_PHASE=\$(sed -n '\(.*\)' \"\\\$CASE_STDERR\".*/\1/p" "$drv")
    echo "$name parses [$(echo "$LINE" | sed -n "$prog" | head -n 1)]"
    echo "$name main debug-on-fail $(grep -c 'debug-on-fail' "$drv")"
    # shellcheck disable=SC2016  # the literal text $FAILED_PHASE is searched for, not expanded
    echo "$name emit-debug-only $(grep -c 'emit-debug-only "\$FAILED_PHASE"' "$drv")"
done

# Drive each driver's own failure-handling block, extracted verbatim from the driver, with a
# stub client that fails the first invocation the way the server does. That makes the guard,
# the diagnostics rerun and the stderr forwarding observable instead of merely present.
for drv in "$CURDIR"/03008_deduplication_*_replicated.sh "$CURDIR"/03008_deduplication_*_nonreplicated.sh; do
    name=$(basename "$drv" .sh)
    W=$(mktemp -d "$CLICKHOUSE_TMP/03008_dedup_probe_XXXXXX")
    cat > "$W/client" <<'STUB'
#!/usr/bin/env bash
D="$(dirname "$0")"
n=$(cat "$D/n" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > "$D/n"
printf '%s' "${!#}" > "$D/sql.$n"
if [ "$n" = 1 ]; then
  echo "Code: 395. DB::Exception: DEDUP_ASSERT_FAILED phase=second table=t: while executing" >&2
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
    echo "$name failure verdict [$out]"
    echo "$name failure client invocations $(cat "$W/n")"
    echo "$name failure rerun is diagnostics-only $( [ -f "$W/sql.2" ] && { grep -c 'DEBUG' "$W/sql.2" || true; } | awk '{print ($1>0)?1:0}' )"
    echo "$name failure rerun mutating $( [ -f "$W/sql.2" ] && { grep -ciE 'DROP |CREATE |INSERT |throwIf' "$W/sql.2" || true; } || echo NA )"
    echo "$name failure rerun phase second $( [ -f "$W/sql.2" ] && { grep -c 'DEBUG second' "$W/sql.2" || true; } )"
    echo "$name failure marker forwarded $(grep -c 'DEDUP_ASSERT_FAILED' "$W/fwd")"
    rm -rf "$W"
done
