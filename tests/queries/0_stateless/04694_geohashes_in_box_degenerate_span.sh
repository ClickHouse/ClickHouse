#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `max_block_size` and `max_threads` are pinned statement-level because the runner randomizes both
# and the effect is per-block: the whole block runs inside one `executeImpl` call.
SETTINGS="max_block_size = 200, max_threads = 1, max_execution_time = 10, timeout_overflow_mode = 'throw'"

# Each row of a box with a zero-item axis walks ~5e8 empty loop iterations unfixed, so 200 rows
# overrun the 10s deadline by orders of magnitude. `ignore` keeps the oracle on termination only:
# what such a box should return is a separate semantics question this test must not pin down.

echo "-- float64, equal latitudes"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ignore(geohashesInBox(materialize(0.), materialize(0.), 180., 0., toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

echo "-- float64, negative zero latitude"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ignore(geohashesInBox(materialize(0.), materialize(0.), 180., -0., toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

echo "-- float32"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ignore(geohashesInBox(materialize(toFloat32(0.)), materialize(toFloat32(0.)), toFloat32(180.), toFloat32(0.), toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

echo "-- equal longitudes, wide latitude span"
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(ignore(geohashesInBox(materialize(0.), materialize(0.), 0., 90., toUInt8(12))))
    FROM numbers(200) SETTINGS $SETTINGS"

echo "-- non-degenerate box result"
${CLICKHOUSE_CLIENT} -q "SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4)"

# Reversed and NaN boxes keep returning an empty array.
echo "-- reversed and nan boxes"
${CLICKHOUSE_CLIENT} -q "SELECT geohashesInBox(1., 1., 0., 0., 4), geohashesInBox(0., 0., nan, 1., 4)"
