#!/usr/bin/env bash
# Regression test for issue #101704
# `clickhouse extract-from-config --try` must still resolve `from_env` substitutions
# when the config has no `<include_from>` tag (or when it has one pointing to a
# non-existent file). Previously, the `--try` codepath skipped all substitutions
# whenever the include_from file was missing, silently turning `from_env` values
# into empty strings and breaking the Docker entrypoint port discovery.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

prefix=$CUR_DIR/"04251_101704_extract_from_config_try_resolves_from_env"

# Scenario 1: from_env with no include_from at all. With --try, env vars set.
# Before the fix: empty output. After the fix: 8123 / 8443 / 9000.
echo "--- 1. from_env without include_from, env set, --try ---"
TEST_HTTP_PORT_101704=8123 TEST_HTTPS_PORT_101704=8443 TEST_NATIVE_PORT_101704=9000 \
    $CLICKHOUSE_BINARY extract-from-config --config-file "$prefix.xml" --key=http_port --try
TEST_HTTP_PORT_101704=8123 TEST_HTTPS_PORT_101704=8443 TEST_NATIVE_PORT_101704=9000 \
    $CLICKHOUSE_BINARY extract-from-config --config-file "$prefix.xml" --key=https_port --try
TEST_HTTP_PORT_101704=8123 TEST_HTTPS_PORT_101704=8443 TEST_NATIVE_PORT_101704=9000 \
    $CLICKHOUSE_BINARY extract-from-config --config-file "$prefix.xml" --key=tcp_port --try

# Scenario 2: Same config, but without --try (must still work as a sanity check).
echo "--- 2. from_env without include_from, env set, no --try ---"
TEST_HTTP_PORT_101704=8123 \
    $CLICKHOUSE_BINARY extract-from-config --config-file "$prefix.xml" --key=http_port

# Scenario 3: from_env with a *bad* include_from. With --try, the missing file is
# ignored AND from_env substitutions still run.
echo "--- 3. from_env with bad include_from, env set, --try ---"
TEST_HTTP_PORT_101704=8123 \
    $CLICKHOUSE_BINARY extract-from-config --config-file "${prefix}_bad_include.xml" --key=http_port --try

# Scenario 4: Missing env var with --try - returns empty without erroring.
echo "--- 4. from_env without include_from, env NOT set, --try ---"
unset TEST_HTTP_PORT_101704
$CLICKHOUSE_BINARY extract-from-config --config-file "$prefix.xml" --key=http_port --try

# Scenario 5: Glob key with from_env, --try - prints all three resolved values.
echo "--- 5. glob *_port with --try ---"
TEST_HTTP_PORT_101704=8123 TEST_HTTPS_PORT_101704=8443 TEST_NATIVE_PORT_101704=9000 \
    $CLICKHOUSE_BINARY extract-from-config --config-file "$prefix.xml" --key='*_port' --try | sort
