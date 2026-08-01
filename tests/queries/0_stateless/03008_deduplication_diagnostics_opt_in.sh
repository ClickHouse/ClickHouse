#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

GEN="$CURDIR/03008_deduplication.python"
ARGS=(--insert-method InsertSelect --table-engine MergeTree --use-insert-token True
      --single-thread True --deduplicate-src-table True --deduplicate-dst-table True
      --insert-unique-blocks True --get-logs false)

SETTINGS_RE='max_insert_threads=1|update_insert_deduplication_token_in_dependent_materialized_views=1|deduplicate_blocks_in_dependent_materialized_views=1|max_block_size=1'

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
        echo "$sub $phase emit probed settings $(echo "$emit" | grep -oE "SET ($SETTINGS_RE)" | sort -u | wc -l)"
        # ... and it cannot destroy or re-create the state it is inspecting.
        echo "$sub $phase emit mutating $(echo "$emit" | grep -ciE 'DROP |CREATE |INSERT |throwIf')"
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
