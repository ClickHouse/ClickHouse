#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: Cannot use CLICKHOUSE_EXTRACT_CONFIG, since it uses default config.

prefix=$CUR_DIR/"03344_clickhouse_extract_from_config_try"

$CLICKHOUSE_BINARY extract-from-config --key merge_tree.comment --config $prefix.xml
$CLICKHOUSE_BINARY extract-from-config --key merge_tree.anything_xml --config $prefix.xml |& grep -o 'Not found: merge_tree.anything_xml'
$CLICKHOUSE_BINARY extract-from-config --key merge_tree.anything_xml --config $prefix.xml --try

$CLICKHOUSE_BINARY extract-from-config --key merge_tree.comment --config $prefix.yaml
$CLICKHOUSE_BINARY extract-from-config --key merge_tree.anything_yaml --config $prefix.yaml |& grep -o 'Not found: merge_tree.anything_yaml'
$CLICKHOUSE_BINARY extract-from-config --key merge_tree.anything_yaml --config $prefix.yaml --try

$CLICKHOUSE_BINARY extract-from-config --key include_from --config ${prefix}_bad_include_from.xml |& grep -o 'File not found: I hope such path will never exists, since it does not even contain a single slash'
$CLICKHOUSE_BINARY extract-from-config --key include_from --config ${prefix}_bad_include_from.xml --try
