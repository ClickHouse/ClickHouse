#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

engine_options=("ReplicatedMergeTree" "MergeTree")
engine=${engine_options[ $RANDOM % ${#engine_options[@]} ]}

insert_method_option=("InsertSelect" "InsertValues")
insert_method=${insert_method_option[ $RANDOM % ${#insert_method_option[@]} ]}

use_insert_token_option=("True" "False")
use_insert_token=${use_insert_token_option[ $RANDOM % ${#use_insert_token_option[@]} ]}

single_thread_options=("True" "False")
single_thread=${single_thread_options[ $RANDOM % ${#single_thread_options[@]} ]}

deduplicate_src_table_options=("True" "False")
deduplicate_src_table=${deduplicate_src_table_options[ $RANDOM % ${#deduplicate_src_table_options[@]} ]}

deduplicate_dst_table_options=("True" "False")
deduplicate_dst_table=${deduplicate_dst_table_options[ $RANDOM % ${#deduplicate_dst_table_options[@]} ]}

insert_unique_blocks_options=("True" "False")
insert_unique_blocks=${insert_unique_blocks_options[ $RANDOM % ${#insert_unique_blocks_options[@]} ]}


THIS_RUN="Test case:"
THIS_RUN+=" insert_method=$insert_method"
THIS_RUN+=" engine=$engine"
THIS_RUN+=" use_insert_token=$use_insert_token"
THIS_RUN+=" single_thread=$single_thread"
THIS_RUN+=" deduplicate_src_table=$deduplicate_src_table"
THIS_RUN+=" deduplicate_dst_table=$deduplicate_dst_table"
THIS_RUN+=" insert_unique_blocks=$insert_unique_blocks"

$CLICKHOUSE_CLIENT --max_insert_block_size 1  -mq "
    $(python3 $CURDIR/03008_deduplication.python insert_several_blocks_into_table \
        --insert-method $insert_method \
        --table-engine $engine \
        --use-insert-token $use_insert_token \
        --single-thread $single_thread \
        --deduplicate-src-table $deduplicate_src_table \
        --deduplicate-dst-table $deduplicate_dst_table \
        --insert-unique-blocks $insert_unique_blocks \
        --get-logs false \
    )
" 1>/dev/null 2>&1 && echo 'insert_several_blocks_into_table OK'  || echo "FAIL: insert_several_blocks_into_table ${THIS_RUN}"

$CLICKHOUSE_CLIENT --max_insert_block_size 1  -mq "
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
" 1>/dev/null 2>&1 && echo 'mv_generates_several_blocks OK'  || echo "FAIL: mv_generates_several_blocks ${THIS_RUN}"

$CLICKHOUSE_CLIENT  --max_insert_block_size 1 -mq "
    $(python3 $CURDIR/03008_deduplication.python several_mv_into_one_table \
        --insert-method $insert_method \
        --table-engine $engine \
        --use-insert-token $use_insert_token \
        --single-thread $single_thread \
        --deduplicate-src-table $deduplicate_src_table \
        --deduplicate-dst-table $deduplicate_dst_table \
        --insert-unique-blocks $insert_unique_blocks \
        --get-logs false \
    )
" 1>/dev/null 2>&1 && echo 'several_mv_into_one_table OK'  || echo "FAIL: several_mv_into_one_table ${THIS_RUN}"

