#!/usr/bin/env bash
# Tags: long, no-fasttest, no-object-storage, no-flaky-check, no-msan
# Tag no-flaky-check -- not compatible with ThreadFuzzer

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ENGINE="ReplicatedMergeTree"

# The diagnostics are no longer part of the success path. Each case's stderr is captured so
# that a failing case can name the phase that failed; it is always forwarded to the real
# stderr afterwards, so the harness still sees it exactly as before.
CASE_STDERR=$(mktemp -p "$CLICKHOUSE_TMP" 03008_dedup_stderr_XXXXXX)
trap 'rm -f "$CASE_STDERR"' EXIT

RUN_ONLY=""
#RUN_ONLY="Test case 0: insert_method=InsertValues engine=ReplicatedMergeTree use_insert_token=True single_thread=True deduplicate_src_table=True deduplicate_dst_table=True insert_unique_blocks=True"

i=0
# Covering design: the full 8x8=64 cross product of "oracle" dimensions
# (insert_unique_blocks, deduplicate_src_table, deduplicate_dst_table) and "path" dimensions
# (insert_method, use_insert_token, single_thread) is collapsed to 16 cases. The expected row
# counts computed by calle() in 03008_deduplication.python depend ONLY on the 3 oracle
# dimensions (8 combinations); insert_method, use_insert_token and single_thread only change
# how the very same INSERT is issued and provably never change the expected counts (see
# commit 602b1285d32, new_unified_hash: identical blocks within one insert are kept, so token
# vs. no-token behaves the same). For each of the 8 oracle combinations we run exactly 2 of the
# 8 path combinations:
#   - "match": the path settings replicate the oracle's own boolean pattern
#     (insert_unique_blocks -> insert_method, deduplicate_src_table -> use_insert_token,
#      deduplicate_dst_table -> single_thread);
#   - "invert": the path settings are the exact opposite of "match" in all 3 dimensions.
# Because "invert" flips every path dimension relative to "match", each oracle row alone already
# exercises both values of every path dimension - so every path dimension value is paired with
# every oracle dimension value. And because inversion is an involution, each of the 8 distinct
# path combinations is used in exactly 2 of the 16 cases overall.
for insert_unique_blocks in "True" "False"; do
    for deduplicate_src_table in "True" "False"; do
        for deduplicate_dst_table in "True" "False"; do
            for variant in "match" "invert"; do
                if [ "$variant" == "match" ]; then
                    if [ "$insert_unique_blocks" == "True" ]; then insert_method="InsertValues"; else insert_method="InsertSelect"; fi
                    use_insert_token=$deduplicate_src_table
                    single_thread=$deduplicate_dst_table
                else
                    if [ "$insert_unique_blocks" == "True" ]; then insert_method="InsertSelect"; else insert_method="InsertValues"; fi
                    if [ "$deduplicate_src_table" == "True" ]; then use_insert_token="False"; else use_insert_token="True"; fi
                    if [ "$deduplicate_dst_table" == "True" ]; then single_thread="False"; else single_thread="True"; fi
                fi

                THIS_RUN="Test case $i:"
                THIS_RUN+=" insert_method=$insert_method"
                THIS_RUN+=" engine=$ENGINE"
                THIS_RUN+=" use_insert_token=$use_insert_token"
                THIS_RUN+=" single_thread=$single_thread"
                THIS_RUN+=" deduplicate_src_table=$deduplicate_src_table"
                THIS_RUN+=" deduplicate_dst_table=$deduplicate_dst_table"
                THIS_RUN+=" insert_unique_blocks=$insert_unique_blocks"

                i=$((i+1))

                echo
                if [ -n "$RUN_ONLY" ] && [ "$RUN_ONLY" != "$THIS_RUN" ]; then
                    echo "skip $THIS_RUN"
                    continue
                fi
                echo "$THIS_RUN"

                        CASE_ARGS=(
                            --insert-method "$insert_method"
                            --table-engine "$ENGINE"
                            --use-insert-token "$use_insert_token"
                            --single-thread "$single_thread"
                            --deduplicate-src-table "$deduplicate_src_table"
                            --deduplicate-dst-table "$deduplicate_dst_table"
                            --insert-unique-blocks "$insert_unique_blocks"
                            --get-logs false
                        )

                        if $CLICKHOUSE_CLIENT --max_insert_block_size 1 -mq "
                            $(python3 "$CURDIR"/03008_deduplication.python mv_generates_several_blocks "${CASE_ARGS[@]}")
                        " 2> "$CASE_STDERR"; then
                            cat "$CASE_STDERR" >&2
                            echo OK
                        else
                            # The client stops at the first error, so the tables the failing
                            # assertion looked at still exist. Re-run only the diagnostics for
                            # that phase, against that surviving state.
                            FAILED_PHASE=$(sed -n 's/.*DEDUP_ASSERT_FAILED phase=\([a-z]*\).*/\1/p' "$CASE_STDERR" | head -n 1)
                            # Forwarded before the rerun, not after: if the per-test deadline
                            # expires while the diagnostics are in flight, the harness is killed
                            # and the EXIT trap deletes CASE_STDERR, so a later cat would lose
                            # the assertion error that names the failing case and phase.
                            cat "$CASE_STDERR" >&2
                            if [ -n "$FAILED_PHASE" ]; then
                                $CLICKHOUSE_CLIENT --max_insert_block_size 1 -mq "
                                    $(python3 "$CURDIR"/03008_deduplication.python mv_generates_several_blocks "${CASE_ARGS[@]}" --emit-debug-only "$FAILED_PHASE")
                                "
                            fi
                            echo FAIL
                        fi
            done
        done
    done
done

echo
echo "All cases executed"
