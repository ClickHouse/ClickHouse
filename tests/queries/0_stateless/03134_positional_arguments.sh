#!/usr/bin/env bash

# Checks that "clickhouse-client/local --help" prints a brief summary of CLI arguments and "--help --verbose" prints all possible CLI arguments
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without --tmp or --path, clickhouse-local locks its default data directory in the home, so parallel tests
# that run it through the bare binary must each get a home of their own.
home_dir="${CLICKHOUSE_TMP}/home_${CLICKHOUSE_DATABASE}"
rm -rf "$home_dir"
mkdir -p "$home_dir"

# The best way to write the query parameter, explicit long option.
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} --query "SELECT 1"

# Shorthand option:
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} -q "SELECT 2"

# It is also accepted as a positional argument
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} "SELECT 3"

# The positional argument can go after normal arguments.
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} --param_test Hello "SELECT {test:String}"

# This is ambiguous: currently works, but does not have to.
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} --query "SELECT 1" "SELECT 2"

# Multiple positional arguments are not allowed.
HOME="$home_dir" XDG_DATA_HOME="" ${CLICKHOUSE_BINARY} "SELECT 1" "SELECT 2" 2>&1 | grep -o -F 'is not supported'

# This is ambiguous - in case of a single word, it can be confused with a tool name.
${CLICKHOUSE_BINARY} "SELECT" 2>&1 | grep -o -F 'Use one of the following commands'

# Everything works with clickhouse/ch/chl and also in clickhouse-local and clickhouse-client.

${CLICKHOUSE_LOCAL} --query "SELECT 1"
${CLICKHOUSE_LOCAL} -q "SELECT 2"
${CLICKHOUSE_LOCAL} "SELECT 3"
${CLICKHOUSE_LOCAL} --param_test Hello "SELECT {test:String}"

${CLICKHOUSE_CLIENT_BINARY} --query "SELECT 1"
${CLICKHOUSE_CLIENT_BINARY} -q "SELECT 2"
${CLICKHOUSE_CLIENT_BINARY} "SELECT 3"
${CLICKHOUSE_CLIENT_BINARY} --param_test Hello "SELECT {test:String}"

rm -rf "$home_dir"
