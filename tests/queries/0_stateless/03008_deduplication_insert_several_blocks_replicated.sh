#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-azure-blob-storage, no-s3-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ENGINE="ReplicatedMergeTree"

RUN_ONLY=""
#RUN_ONLY="Test case 52: insert_method=InsertSelect engine=ReplicatedMergeTree use_insert_token=False single_thread=True deduplicate_src_table=False deduplicate_dst_table=True insert_unique_blocks=True"

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

                        $CLICKHOUSE_CLIENT --max_insert_block_size 1  -mq "
                            $(python3 $CURDIR/03008_deduplication.python insert_several_blocks_into_table \
                                --insert-method $insert_method \
                                --table-engine $ENGINE \
                                --use-insert-token $use_insert_token \
                                --single-thread $single_thread \
                                --deduplicate-src-table $deduplicate_src_table \
                                --deduplicate-dst-table $deduplicate_dst_table \
                                --insert-unique-blocks $insert_unique_blocks \
                                --get-logs false \
                            )
                        " && echo OK || echo FAIL
                    done
                done
            done
        done
    done
done

echo
echo "All cases executed"
