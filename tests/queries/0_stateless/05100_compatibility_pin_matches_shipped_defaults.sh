#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each pin is asserted on both sides of the release that shipped the new default, so the entries
# stay in their own version block: deleting them outright would also satisfy a one-sided check.
# `MergeTree` defaults are resolved once per process from the profile's `compatibility`
# (`Context::getMergeTreeSettings` caches them), so the pin has to come from the command line
# rather than from `SET`.
for pin in 26.8 26.7 26.1 25.12; do
    echo "-- compatibility = $pin"
    $CLICKHOUSE_LOCAL --compatibility="$pin" -q "
        SELECT name, value FROM
        (
            SELECT name, value FROM system.settings WHERE name IN (
                'distributed_cache_min_inflight_bytes_to_discard_connection_on_seek',
                'distributed_plan_workers_provisioning_timeout_ms',
                'query_plan_aggregation_bucket_top_k',
                'query_plan_optimize_lazy_materialization_for_object_storage',
                'use_skip_indexes_on_data_read')
            UNION ALL
            SELECT name, value FROM system.merge_tree_settings WHERE name = 'patch_parts_version'
        )
        ORDER BY name"
done

# The recorded history is published through `system.settings_changes` and the generated reference
# docs, so a duplicate copy in another block is user-visible even where it changes no pin outcome.
echo "-- recorded history"
$CLICKHOUSE_LOCAL -q "
    SELECT type, version, c.name, c.previous_value, c.new_value
    FROM (SELECT type, version, arrayJoin(changes) AS c FROM system.settings_changes)
    WHERE c.name IN (
        'distributed_cache_min_inflight_bytes_to_discard_connection_on_seek',
        'distributed_plan_workers_provisioning_timeout_ms',
        'patch_parts_version',
        'query_plan_aggregation_bucket_top_k',
        'query_plan_optimize_lazy_materialization_for_object_storage',
        'use_skip_indexes_on_data_read')
    ORDER BY c.name, type, version"
