#!/usr/bin/env bash
# A configuration file can be written in XML or in YAML, so `clickhouse-keeper-client` must pick up
# `config.yaml` and `config.yml` from the current directory just like `config.xml`.
#
# The connection itself is not attempted against a real Keeper: it is enough to see that the node from
# the configuration file was taken for the connection, which the client reports in its log.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TESTDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_keeper_client_config_yaml"
trap 'rm -rf "$TESTDIR"' EXIT

rm -rf "$TESTDIR"
mkdir -p "$TESTDIR"

write_config()
{
    cat > "$1" <<EOF
zookeeper:
    node:
        host: 255.255.255.255
        port: 1
EOF
}

for name in config.yaml config.yml config.xml
do
    echo "-- ./$name"
    rm -f "$TESTDIR"/config.*
    if [ "$name" = config.xml ]
    then
        cat > "$TESTDIR/$name" <<EOF
<clickhouse>
    <zookeeper>
        <node>
            <host>255.255.255.255</host>
            <port>1</port>
        </node>
    </zookeeper>
</clickhouse>
EOF
    else
        write_config "$TESTDIR/$name"
    fi
    (
        cd "$TESTDIR" || exit 1
        timeout 60 "${CLICKHOUSE_BINARY}" keeper-client --log-level information --connection-timeout 1 \
            --query "ls /" 2>&1 | grep -c -F "Found keeper node in $name"
    )
done
