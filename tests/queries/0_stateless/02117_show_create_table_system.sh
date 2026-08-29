#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

# System table comments contain their complete reference documentation. This
# test covers the table definitions, so omit the trailing comments.
tables=(
    aggregate_function_combinators
    asynchronous_inserts
    asynchronous_metrics
    build_options
    clusters
    collations
    columns
    contributors
    current_roles
    data_skipping_index_types
    data_skipping_indices
    data_type_families
    databases
    detached_parts
    dictionaries
    dictionary_layouts
    disks
    distributed_ddl_queue
    distribution_queue
    enabled_roles
    errors
    events
    formats
    functions
    graphite_retentions
    licenses
    macros
    merge_tree_settings
    merges
    metrics
    moves
    mutations
    numbers
    numbers_mt
    one
    part_moves_between_shards
    parts
    parts_columns
    processes
    projection_parts
    projection_parts_columns
    quota_limits
    quota_usage
    quotas
    quotas_usage
    replicas
    replicated_fetches
    replicated_merge_tree_settings
    replication_queue
    role_grants
    roles
    row_policies
    settings
    settings_profile_elements
    settings_profiles
    stack_trace
    storage_policies
    table_engines
    table_functions
    tables
    time_zones
    user_directories
    users
    warnings
    zeros
    zeros_mt
)

for table in "${tables[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE system.${table} FORMAT TSVRaw" | sed '/^COMMENT /,$d'
done
