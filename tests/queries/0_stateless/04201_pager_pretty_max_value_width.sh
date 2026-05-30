#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test: when --pager is specified, output_format_pretty_max_value_width is unlimited.
# Covers: src/Client/ClientBase.cpp lines 887-891 (the value-width override branch).
# Default output_format_pretty_max_value_width=10000; force apply_for_single_value=1
# so single-value bypass does not hide truncation. Without the override, an 11000-char
# value would be truncated; with the override, the full value is emitted.

# 1) With --pager: full value (no truncation).
$CLICKHOUSE_CLIENT --pager 'cat' \
    --output_format_pretty_max_value_width_apply_for_single_value 1 \
    --query "SELECT repeat('a', 11000) FORMAT Pretty" | wc -c

# 2) User-supplied output_format_pretty_max_value_width must be preserved
#    (the override checks .changed and skips the override when user set it).
$CLICKHOUSE_CLIENT --pager 'cat' \
    --output_format_pretty_max_value_width 100 \
    --output_format_pretty_max_value_width_apply_for_single_value 1 \
    --query "SELECT repeat('a', 11000) FORMAT Pretty" | grep -c '⋯'
