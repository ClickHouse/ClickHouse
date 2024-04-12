#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test case 8: engine=MergeTree use_insert_token=True single_thread=False deduplicate_src_table=True deduplicate_dst_table=True insert_unique_blocks=True
# Test case 9: engine=MergeTree use_insert_token=True single_thread=False deduplicate_src_table=True deduplicate_dst_table=True insert_unique_blocks=False
# Test case 10: engine=MergeTree use_insert_token=True single_thread=False deduplicate_src_table=True deduplicate_dst_table=False insert_unique_blocks=True
# Test case 11: engine=MergeTree use_insert_token=True single_thread=False deduplicate_src_table=True deduplicate_dst_table=False insert_unique_blocks=False
# failed due to race in multi thread insertion, blocks are deduplicated in different threads

# Test case 12: engine=MergeTree use_insert_token=True single_thread=False deduplicate_src_table=False deduplicate_dst_table=True insert_unique_blocks=True
# Test case 13: engine=MergeTree use_insert_token=True single_thread=False deduplicate_src_table=False deduplicate_dst_table=True insert_unique_blocks=False
# the same as first but for dst table

# Test case 16: engine=MergeTree use_insert_token=False single_thread=True deduplicate_src_table=True deduplicate_dst_table=True insert_unique_blocks=True
# Test case 20: engine=MergeTree use_insert_token=False single_thread=True deduplicate_src_table=False deduplicate_dst_table=True insert_unique_blocks=True
# Test case 24: engine=MergeTree use_insert_token=False single_thread=False deduplicate_src_table=True deduplicate_dst_table=True insert_unique_blocks=True
# Test case 28: engine=MergeTree use_insert_token=False single_thread=False deduplicate_src_table=False deduplicate_dst_table=True insert_unique_blocks=True
# dst table deduplicates all incoming blocks from one insert because not uniq hash

RUN_ONLY=""
#RUN_ONLY="Test case 20: engine=MergeTree use_insert_token=False single_thread=True deduplicate_src_table=False deduplicate_dst_table=True insert_unique_blocks=True"

KNOWN_ERRORS=(8 9 10 11 12 13 16 20 24 28)

function is_known_error()
{
    n=$1
    for e in "${KNOWN_ERRORS[@]}"; do
      if [ "$n" -eq "$e" ] || [ "$n" -eq "$((e+32))" ] || [ "$n" -eq "$((e+64))" ] || [ "$n" -eq "$((e+64+32))" ]; then
        return 0
      fi
    done
    return 1
}

i=0
for insert_method in "InsertSelect" "InsertValues"; do
    for engine in "MergeTree" "ReplicatedMergeTree"; do
        for use_insert_token in "True" "False"; do
            for single_thread in "True" "False"; do
                for deduplicate_src_table in "True" "False"; do
                    for deduplicate_dst_table in "True" "False"; do
                        for insert_unique_blocks in "True" "False"; do

                            THIS_RUN="Test case $i:"
                            THIS_RUN+=" insert_method=$insert_method"
                            THIS_RUN+=" engine=$engine"
                            THIS_RUN+=" use_insert_token=$use_insert_token"
                            THIS_RUN+=" single_thread=$single_thread"
                            THIS_RUN+=" deduplicate_src_table=$deduplicate_src_table"
                            THIS_RUN+=" deduplicate_dst_table=$deduplicate_dst_table"
                            THIS_RUN+=" insert_unique_blocks=$insert_unique_blocks"

                            is_error=$(is_known_error "$i" && echo Y || echo N)
                            i=$((i+1))

                            echo
                            if [ -n "$RUN_ONLY" ] && [ "$RUN_ONLY" != "$THIS_RUN" ]; then
                              echo "skip $THIS_RUN"
                              continue
                            fi
                            echo "$THIS_RUN"

                            if [ "$is_error" = Y ]; then
                                $CLICKHOUSE_CLIENT --max_insert_block_size 1 -nmq "
                                  $(python3 $CURDIR/03008_deduplication.python mv_generates_several_blocks \
                                      --insert-method $insert_method \
                                      --table-engine $engine \
                                      --use-insert-token $use_insert_token \
                                      --single-thread $single_thread \
                                      --deduplicate-src-table $deduplicate_src_table \
                                      --deduplicate-dst-table $deduplicate_dst_table \
                                      --insert-unique-blocks $insert_unique_blocks \
                                      --get-logs false \
                                  )
                                " 2>/dev/null && echo FIXED || echo EXPECTED_TO_FAIL
                            else
                                $CLICKHOUSE_CLIENT --max_insert_block_size 1  -nmq "
                                  $(python3 $CURDIR/03008_deduplication.python mv_generates_several_blocks \
                                      --insert-method $insert_method \
                                      --table-engine $engine \
                                      --use-insert-token $use_insert_token \
                                      --single-thread $single_thread \
                                      --deduplicate-src-table $deduplicate_src_table \
                                      --deduplicate-dst-table $deduplicate_dst_table \
                                      --insert-unique-blocks $insert_unique_blocks \
                                      --get-logs false \
                                  )
                                " && echo OK || echo FAIL
                            fi
                        done
                    done
                done
            done
        done
    done
done

echo
echo "All cases executed"
