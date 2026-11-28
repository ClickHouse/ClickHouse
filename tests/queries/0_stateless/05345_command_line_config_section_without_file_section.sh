#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A section given only on the command line must apply although the configuration file has no such section.
CONFIG="${CLICKHOUSE_TMP}/$(basename "${BASH_SOURCE[0]}" .sh).xml"
echo '<clickhouse/>' > "$CONFIG"
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --query "SELECT value, changed FROM system.merge_tree_settings WHERE name = 'max_suspicious_broken_parts'" -- --merge_tree.max_suspicious_broken_parts=7
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --query "SELECT value, changed FROM system.replicated_merge_tree_settings WHERE name = 'max_suspicious_broken_parts'" -- --replicated_merge_tree.max_suspicious_broken_parts=9
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --query "SELECT getMacro('test')" -- --macros.test=value
rm -f "$CONFIG"
