#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-flaky-check, no-msan
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
#RUN_ONLY="Test case 20: engine=ReplicatedMergeTree use_insert_token=False single_thread=True deduplicate_src_table=False deduplicate_dst_table=True insert_unique_blocks=True"

i=0
for insert_method in "InsertSelect" "InsertValues"; do
    for use_insert_token in "True" "False"; do
        for single_thread in "True" "False"; do
            for deduplicate_src_table in "True" "False"; do
                for deduplicate_dst_table in "True" "False"; do
                    for insert_unique_blocks in "True" "False"; do

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
    done
done

echo
echo "All cases executed"
