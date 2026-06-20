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
