#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --query "
CREATE DICTIONARY d0.\`d213\` (\`c0\` Int128 DEFAULT '{\"c0.c1\":[],\"😉😉\":17453198.019}' HIERARCHICAL, \`c1\` IPv6 DEFAULT +inf, \`c2\` Int32 DEFAULT '\\'\\xE6\\xBC\\x82\\xE4\\xBA\\xAE\\'' INJECTIVE, \`c3\` Int128 DEFAULT +inf EXPRESSION (\`c1\` AS \`a0\`) HIERARCHICAL) PRIMARY KEY (\`c3\`, \`c0\`, \`c1\`) SOURCE(CLICKHOUSE(DB 'd0' TABLE 'd109')) LAYOUT(RANGE_HASHED()) RANGE(MIN \`c2\`MAX \`c1\`) LIFETIME(MIN 2 MAX 30) SETTINGS(allow_prefetched_read_pool_for_local_filesystem = 1, max_compress_block_size = 524288, join_on_disk_max_files_to_merge = 10, materialized_views_ignore_errors = 0, alter_partition_verbose_result = 0, query_plan_optimize_prewhere = 0, input_format_arrow_case_insensitive_column_matching = 0, input_format_parallel_parsing = 1, output_format_protobuf_nullables_with_google_wrappers = 1, input_format_allow_errors_num = 64, use_page_cache_with_distributed_cache = 1, optimize_trivial_insert_select = 1, parallel_replicas_custom_key_range_lower = 524288, filesystem_cache_enable_background_download_during_fetch = 1, max_parts_to_move = 2837, optimize_throw_if_noop = 0, validate_experimental_and_suspicious_types_inside_nested_types = 0, force_optimize_projection = 1, max_insert_threads = 4) COMMENT '日本' PARALLEL WITH SYSTEM DISABLE FAILPOINT zero_copy_lock_zk_fail_before_op;
" | $CLICKHOUSE_FORMAT
