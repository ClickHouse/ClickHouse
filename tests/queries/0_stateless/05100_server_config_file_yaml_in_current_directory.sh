#!/usr/bin/env bash
# A configuration file can be written in XML or in YAML, so `clickhouse-server` must find `config.yaml`
# and `config.yml` in the current directory just like `config.xml`, instead of silently falling back to
# the configuration embedded into the binary.
#
# The configuration files below are intentionally malformed: this way the server reports the file it read
# and exits immediately, without starting up and without binding any ports.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TESTDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_server_config_yaml"
trap 'rm -rf "$TESTDIR"' EXIT

rm -rf "$TESTDIR"
mkdir -p "$TESTDIR"

for name in config.yaml config.yml
do
    echo "-- ./$name"
    rm -f "$TESTDIR"/config.*
    echo "max_thread_pool_size: [1001" > "$TESTDIR/$name"
    (
        cd "$TESTDIR" || exit 1
        # `timeout` guards against the server ignoring the file and starting up instead of failing.
        timeout 60 ${CLICKHOUSE_SERVER_BINARY} 2>&1 | grep -c -F "Unable to parse YAML configuration file $name"
    )
done
