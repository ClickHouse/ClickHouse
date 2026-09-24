#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.projection_parts.default_compression_codec` must not present a guess as the codec of a
# projection part: a codec that could only be recovered from the projection data is reported as
# 'UNKNOWN', and a projection part that could not be loaded at all reports no codec.

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${data_path:?}"

$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
CREATE TABLE tab (a UInt64, b String, PROJECTION p (SELECT b ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS default_compression_codec = 'ZSTD(3)', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO tab SELECT number, toString(number) FROM numbers(1000);
"

part_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY name")

echo '-- the codec of the projection part is known'
$CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT name, is_broken, default_compression_codec FROM system.projection_parts WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY name"

rm "${part_path:?}p.proj/default_compression_codec.txt"

echo '-- codec file gone: the codec recovered from the projection data is reported as unknown'
$CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT name, is_broken, default_compression_codec FROM system.projection_parts WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY name"

rm -rf "${part_path:?}p.proj"

echo '-- projection part could not be loaded: broken, with no codec'
$CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT name, is_broken, empty(default_compression_codec) FROM system.projection_parts WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY name"

rm -rf "${data_path:?}"
