#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir=$(mktemp -d "${CLICKHOUSE_TMP}/legacy_bucketed_map.XXXXXX")
trap 'rm -rf "$workdir"' EXIT

part_dir=$($CLICKHOUSE_LOCAL --path "$workdir" --multiquery --query "
CREATE DATABASE legacy ENGINE = Atomic;
CREATE TABLE legacy.maps (j JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, object_shared_data_buckets_for_wide_part = 1,
    object_shared_data_serialization_version = 'advanced', object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    propagate_types_serialization_versions_to_nested_types = 1;
SELECT concat(data_paths[1], 'detached') FROM system.tables WHERE database = 'legacy' AND name = 'maps';
")

# A real part written by `91ffe225686`, before the `Map` statistics flag fix.
# Its `JSON` paths contain a scalar-valued map, a tuple-valued map, an array of maps,
# and nested `JSON` with a typed map. The original checksums are preserved.
base64 -d <<'PART' | tar -xz -C "$part_dir"
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
PART

$CLICKHOUSE_LOCAL --path "$workdir" --multiquery <<'SQL'
ALTER TABLE legacy.maps ATTACH PART 'all_1_1_0';
SELECT 'legacy', j.m, j.t, j.a, j.o FROM legacy.maps;
SELECT 'subcolumns', j.m.:`Map(String, UInt64)`['k'], j.t.:`Map(String, Tuple(a UInt64))`['k'].a FROM legacy.maps;
INSERT INTO legacy.maps SELECT j FROM legacy.maps;
OPTIMIZE TABLE legacy.maps FINAL;
SELECT 'rewritten', j.m, j.t, j.a, j.o FROM legacy.maps;
SQL
