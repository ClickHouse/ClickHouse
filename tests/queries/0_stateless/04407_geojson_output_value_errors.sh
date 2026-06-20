#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Values that cannot be represented in GeoJSON are rejected mid-stream. The partial output before the
# error is discarded, so each query is checked for failure rather than by comparing its output.

# A GeoJSON position must consist of JSON numbers. Non-finite coordinates are rejected both by default
# (where a non-finite number would otherwise be written as `null`) and with quote_denormals enabled
# (where it would otherwise be written as a quoted token).
${CLICKHOUSE_CLIENT} --query "SELECT (nan, 2.0)::Point AS geometry FORMAT GeoJSON" >/dev/null 2>&1 \
    && echo "nan coordinate accepted" || echo "nan coordinate rejected"
${CLICKHOUSE_CLIENT} --query "SELECT (1.0, inf)::Point AS geometry FORMAT GeoJSON SETTINGS output_format_json_quote_denormals = 1" >/dev/null 2>&1 \
    && echo "inf coordinate accepted" || echo "inf coordinate rejected"

# A floating-point feature id must be finite; a non-finite id is rejected rather than written as null.
${CLICKHOUSE_CLIENT} --query "SELECT nan AS id, (1.0, 2.0)::Point AS geometry FORMAT GeoJSON" >/dev/null 2>&1 \
    && echo "nan id accepted" || echo "nan id rejected"

# A GeoJSON LineString (and each line of a MultiLineString) must have at least two positions; a shorter
# line is rejected rather than emitted as a document the input format would reject.
${CLICKHOUSE_CLIENT} --query "SELECT [(0.0, 0.0)]::LineString AS geometry FORMAT GeoJSON" >/dev/null 2>&1 \
    && echo "one-point linestring accepted" || echo "one-point linestring rejected"
${CLICKHOUSE_CLIENT} --query "SELECT [[(0.0, 0.0)]]::MultiLineString AS geometry FORMAT GeoJSON" >/dev/null 2>&1 \
    && echo "one-point multilinestring accepted" || echo "one-point multilinestring rejected"
