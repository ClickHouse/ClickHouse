#!/bin/bash

# script allows to install configs for clickhouse server and clients required
# for testing (stateless and stateful tests)

set -x -e

DEST_SERVER_PATH="${1:-/etc/clickhouse-server}"
DEST_CLIENT_PATH="${2:-/etc/clickhouse-client}"
SRC_PATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

echo "Going to install test configs from $SRC_PATH into $DEST_SERVER_PATH"

mkdir -p $DEST_SERVER_PATH/config.d/
mkdir -p $DEST_SERVER_PATH/users.d/
mkdir -p $DEST_CLIENT_PATH

ln -sf $SRC_PATH/config.d/zookeeper.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/listen.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/part_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/text_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/metric_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/custom_settings_prefixes.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/macros.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/disks.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/secure_ports.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/clusters.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/graphite.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/database_atomic.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/test_cluster_with_incorrect_pw.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/logging_no_rotate.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/tcp_with_proxy.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/users.d/log_queries.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/readonly.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/access_management.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/database_atomic_drop_detach_sync.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/opentelemetry.xml $DEST_SERVER_PATH/users.d/

ln -sf $SRC_PATH/ints_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/strings_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/decimals_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/executable_dictionary.xml $DEST_SERVER_PATH/

ln -sf $SRC_PATH/server.key $DEST_SERVER_PATH/
ln -sf $SRC_PATH/server.crt $DEST_SERVER_PATH/
ln -sf $SRC_PATH/dhparam.pem $DEST_SERVER_PATH/

# Retain any pre-existing config and allow ClickHouse to load it if required
ln -sf --backup=simple --suffix=_original.xml \
   $SRC_PATH/config.d/query_masking_rules.xml $DEST_SERVER_PATH/config.d/

if [[ -n "$USE_POLYMORPHIC_PARTS" ]] && [[ "$USE_POLYMORPHIC_PARTS" -eq 1 ]]; then
    ln -sf $SRC_PATH/config.d/polymorphic_parts.xml $DEST_SERVER_PATH/config.d/
fi
if [[ -n "$USE_DATABASE_ORDINARY" ]] && [[ "$USE_DATABASE_ORDINARY" -eq 1 ]]; then
    ln -sf $SRC_PATH/users.d/database_ordinary.xml $DEST_SERVER_PATH/users.d/
fi

ln -sf $SRC_PATH/client_config.xml $DEST_CLIENT_PATH/config.xml
