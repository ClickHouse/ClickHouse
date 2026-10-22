#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# `compatibility = '24.10'` gives the server-wide `MergeTree` settings their old defaults, except those of a
# tier `allow_feature_tier` disables: `compute_exact_num_defaults_for_sparse_columns` is BETA, and
# `replicated_deduplication_window` is PRODUCTION.
for tier in 2 3
do
    echo "allow_feature_tier = ${tier}"
    ${CLICKHOUSE_LOCAL} --compatibility 24.10 -q "
        SELECT name, value = default FROM system.merge_tree_settings
        WHERE name IN ('compute_exact_num_defaults_for_sparse_columns', 'replicated_deduplication_window')
        ORDER BY name" -- --allow_feature_tier="${tier}"
done
