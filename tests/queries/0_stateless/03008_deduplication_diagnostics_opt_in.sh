#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest -- runs the shared 03008_deduplication.python generator, which the Fast
# test build does not need to exercise.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

GEN="$CURDIR/03008_deduplication.python"
ARGS=(--insert-method InsertSelect --table-engine MergeTree --use-insert-token True
      --single-thread True --deduplicate-src-table True --deduplicate-dst-table True
      --insert-unique-blocks True --get-logs false)

for sub in insert_several_blocks_into_table mv_generates_several_blocks several_mv_into_one_table; do
    main=$(python3 "$GEN" "$sub" "${ARGS[@]}")
    # The success path carries no diagnostics at all.
    echo "$sub main DEBUG $(echo "$main" | grep -c 'DEBUG')"
    # Every assertion names its phase, so the driver can dispatch (2 tables x 2 phases).
    echo "$sub main markers $(echo "$main" | grep -c 'DEDUP_ASSERT_FAILED')"

    for phase in first second; do
        emit=$(python3 "$GEN" "$sub" "${ARGS[@]}" --emit-debug-only "$phase")
        # The diagnostics-only script carries the diagnostics for THAT phase ...
        echo "$sub $phase emit DEBUG $(echo "$emit" | grep -c "DEBUG $phase")"
        # ... applies the session settings the diagnostics report on ...
        echo "$sub $phase emit SET $(echo "$emit" | grep -cE '^[[:space:]]*SET ')"
        # ... and cannot destroy or re-create the state it is inspecting.
        echo "$sub $phase emit mutating $(echo "$emit" | grep -ciE 'DROP |CREATE |INSERT |throwIf')"
    done
done
