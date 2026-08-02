#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `max_block_size` and `max_threads` are pinned statement-level because the runner randomizes both
# and the effect is per-block: the whole block runs inside one `executeImpl` call.
SETTINGS="max_block_size = 200, max_threads = 1, max_execution_time = 10, timeout_overflow_mode = 'throw'"

# A zero-area box yields one geohash per row. Unfixed, each row also walks ~5e8 empty loop
# iterations, so 200 rows overrun the 10s deadline by orders of magnitude and report a timeout.

echo "-- float64, equal latitudes"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(length(geohashesInBox(materialize(0.), materialize(0.), 180., 0., toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

echo "-- float64, negative zero latitude"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(length(geohashesInBox(materialize(0.), materialize(0.), 180., -0., toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

echo "-- float32"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(length(geohashesInBox(materialize(toFloat32(0.)), materialize(toFloat32(0.)), toFloat32(180.), toFloat32(0.), toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

echo "-- equal longitudes, wide latitude span"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(length(geohashesInBox(materialize(0.), materialize(0.), 0., 90., toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

# A zero-area box must still return the single geohash it degenerates to, not an empty array.
echo "-- degenerate box result"
${CLICKHOUSE_CLIENT} -q "SELECT geohashesInBox(0., 0., 180., 0., 12), geohashesInBox(0., 0., 0., 90., 12)"

echo "-- non-degenerate box result"
${CLICKHOUSE_CLIENT} -q "SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4)"

# Reversed and NaN boxes keep returning an empty array.
echo "-- reversed and nan boxes"
${CLICKHOUSE_CLIENT} -q "SELECT geohashesInBox(1., 1., 0., 0., 4), geohashesInBox(0., 0., nan, 1., 4)"
