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

ln -sf $SRC_PATH/config.d/zookeeper_write.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/max_num_to_warn.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/listen.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/text_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/blob_storage_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/custom_settings_prefixes.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/database_catalog_drop_table_concurrency.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/enable_access_control_improvements.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/macros.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/secure_ports.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/clusters.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/graphite.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/graphite_alternative.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/grpc_protocol.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/database_atomic.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/max_concurrent_queries.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/merge_tree_settings.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/backoff_failed_mutation.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/merge_tree_old_dirs_cleanup.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/test_cluster_with_incorrect_pw.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/keeper_port.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/logging_no_rotate.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/merge_tree.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/lost_forever_check.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/tcp_with_proxy.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/prometheus.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/top_level_domains_lists.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/top_level_domains_path.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/transactions.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/encryption.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/CORS.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/zookeeper_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/logger_trace.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/named_collection.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/ssl_certs.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/filesystem_cache_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/session_log.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/system_unfreeze.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/enable_zero_copy_replication.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/nlp.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/forbidden_headers.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/enable_keeper_map.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/custom_disks_base_path.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/display_name.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/compressed_marks_and_index.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/disable_s3_env_credentials.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/enable_wait_for_shutdown_replicated_tables.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/backups.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/filesystem_caches_path.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/validate_tcp_client_information.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/zero_copy_destructive_operations.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/block_number.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/handlers.yaml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/serverwide_trace_collector.xml $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/config.d/rocksdb.xml $DEST_SERVER_PATH/config.d/

# Not supported with fasttest.
if [ "${DEST_SERVER_PATH}" = "/etc/clickhouse-server" ]
then
   ln -sf $SRC_PATH/config.d/legacy_geobase.xml $DEST_SERVER_PATH/config.d/
fi

ln -sf $SRC_PATH/users.d/log_queries.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/readonly.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/access_management.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/database_atomic_drop_detach_sync.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/opentelemetry.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/remote_queries.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/session_log_test.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/memory_profiler.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/no_fsync_metadata.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/filelog.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/enable_blobs_check.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/marks.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/insert_keeper_retries.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/prefetch_settings.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/nonconst_timezone.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/allow_introspection_functions.yaml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/replicated_ddl_entry.xml $DEST_SERVER_PATH/users.d/
ln -sf $SRC_PATH/users.d/limits.yaml $DEST_SERVER_PATH/users.d/

if [[ -n "$USE_OLD_ANALYZER" ]] && [[ "$USE_OLD_ANALYZER" -eq 1 ]]; then
    ln -sf $SRC_PATH/users.d/analyzer.xml $DEST_SERVER_PATH/users.d/
fi

# FIXME DataPartsExchange may hang for http_send_timeout seconds
# when nobody is going to read from the other side of socket (due to "Fetching of part was cancelled"),
# but socket is owned by HTTPSessionPool, so it's not closed.
ln -sf $SRC_PATH/users.d/timeouts.xml $DEST_SERVER_PATH/users.d/

ln -sf $SRC_PATH/ints_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/strings_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/decimals_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/executable_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/executable_pool_dictionary.xml $DEST_SERVER_PATH/
ln -sf $SRC_PATH/test_function.xml $DEST_SERVER_PATH/

ln -sf $SRC_PATH/top_level_domains $DEST_SERVER_PATH/
ln -sf $SRC_PATH/regions_hierarchy.txt $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/regions_names_en.txt $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/regions_names_es.txt $DEST_SERVER_PATH/config.d/

ln -sf $SRC_PATH/ext-en.txt $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/ext-ru.txt $DEST_SERVER_PATH/config.d/
ln -sf $SRC_PATH/lem-en.bin $DEST_SERVER_PATH/config.d/

ln -sf $SRC_PATH/server.key $DEST_SERVER_PATH/
ln -sf $SRC_PATH/server.crt $DEST_SERVER_PATH/
ln -sf $SRC_PATH/dhparam.pem $DEST_SERVER_PATH/

# Retain any pre-existing config and allow ClickHouse to load it if required
ln -sf --backup=simple --suffix=_original.xml \
   $SRC_PATH/config.d/query_masking_rules.xml $DEST_SERVER_PATH/config.d/

if [[ -n "$ZOOKEEPER_FAULT_INJECTION" ]] && [[ "$ZOOKEEPER_FAULT_INJECTION" -eq 1 ]]; then
    rm -f $DEST_SERVER_PATH/config.d/zookeeper.xml ||:
    ln -sf $SRC_PATH/config.d/zookeeper_fault_injection.xml $DEST_SERVER_PATH/config.d/
else
    rm -f $DEST_SERVER_PATH/config.d/zookeeper_fault_injection.xml ||:
    ln -sf $SRC_PATH/config.d/zookeeper.xml $DEST_SERVER_PATH/config.d/
fi

if [[ -n "$THREAD_POOL_FAULT_INJECTION" ]] && [[ "$THREAD_POOL_FAULT_INJECTION" -eq 1 ]]; then
    ln -sf $SRC_PATH/config.d/cannot_allocate_thread_injection.xml $DEST_SERVER_PATH/config.d/
else
    rm -f $DEST_SERVER_PATH/config.d/cannot_allocate_thread_injection.xml ||:
fi

# We randomize creating the snapshot on exit for Keeper to test out using older snapshots
value=$(($RANDOM % 2))
sed --follow-symlinks -i "s|<create_snapshot_on_exit>[01]</create_snapshot_on_exit>|<create_snapshot_on_exit>$value</create_snapshot_on_exit>|" $DEST_SERVER_PATH/config.d/keeper_port.xml

value=$((($RANDOM + 100) * 2048))
sed --follow-symlinks -i "s|<latest_logs_cache_size_threshold>[[:digit:]]\+</latest_logs_cache_size_threshold>|<latest_logs_cache_size_threshold>$value</latest_logs_cache_size_threshold>|" $DEST_SERVER_PATH/config.d/keeper_port.xml

value=$((($RANDOM + 100) * 2048))
sed --follow-symlinks -i "s|<commit_logs_cache_size_threshold>[[:digit:]]\+</commit_logs_cache_size_threshold>|<commit_logs_cache_size_threshold>$value</commit_logs_cache_size_threshold>|" $DEST_SERVER_PATH/config.d/keeper_port.xml

if [[ -n "$USE_POLYMORPHIC_PARTS" ]] && [[ "$USE_POLYMORPHIC_PARTS" -eq 1 ]]; then
    ln -sf $SRC_PATH/config.d/polymorphic_parts.xml $DEST_SERVER_PATH/config.d/
fi
if [[ -n "$USE_DATABASE_ORDINARY" ]] && [[ "$USE_DATABASE_ORDINARY" -eq 1 ]]; then
    ln -sf $SRC_PATH/users.d/database_ordinary.xml $DEST_SERVER_PATH/users.d/
fi

if [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" == "1" ]]; then
    object_key_types_options=("generate-suffix" "generate-full-key" "generate-template-key")
    object_key_type="${object_key_types_options[0]}"

    if [[ -n "$RANDOMIZE_OBJECT_KEY_TYPE" ]] && [[ "$RANDOMIZE_OBJECT_KEY_TYPE" -eq 1 ]]; then
      object_key_type="${object_key_types_options[$(($RANDOM % ${#object_key_types_options[@]}))]}"
    fi

    case $object_key_type in
        "generate-full-key")
            ln -sf $SRC_PATH/config.d/storage_metadata_with_full_object_key.xml $DEST_SERVER_PATH/config.d/
            ln -sf $SRC_PATH/config.d/s3_storage_policy_by_default.xml $DEST_SERVER_PATH/config.d/
            ;;
        "generate-template-key")
            ln -sf $SRC_PATH/config.d/storage_metadata_with_full_object_key.xml $DEST_SERVER_PATH/config.d/
            ln -sf $SRC_PATH/config.d/s3_storage_policy_with_template_object_key.xml $DEST_SERVER_PATH/config.d/s3_storage_policy_by_default.xml
            ;;
        "generate-suffix"|*)
            ln -sf $SRC_PATH/config.d/s3_storage_policy_by_default.xml $DEST_SERVER_PATH/config.d/
            ;;
    esac
elif [[ "$USE_AZURE_STORAGE_FOR_MERGE_TREE" == "1" ]]; then
    ln -sf $SRC_PATH/config.d/azure_storage_policy_by_default.xml $DEST_SERVER_PATH/config.d/
fi

if [[ -n "$EXPORT_S3_STORAGE_POLICIES" ]]; then
    ln -sf $SRC_PATH/config.d/azure_storage_conf.xml $DEST_SERVER_PATH/config.d/
    ln -sf $SRC_PATH/config.d/storage_conf.xml $DEST_SERVER_PATH/config.d/
    ln -sf $SRC_PATH/config.d/storage_conf_02944.xml $DEST_SERVER_PATH/config.d/
    ln -sf $SRC_PATH/config.d/storage_conf_02963.xml $DEST_SERVER_PATH/config.d/
    ln -sf $SRC_PATH/config.d/storage_conf_02961.xml $DEST_SERVER_PATH/config.d/
    ln -sf $SRC_PATH/users.d/s3_cache.xml $DEST_SERVER_PATH/users.d/
    ln -sf $SRC_PATH/users.d/s3_cache_new.xml $DEST_SERVER_PATH/users.d/
fi

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    ln -sf $SRC_PATH/users.d/database_replicated.xml $DEST_SERVER_PATH/users.d/
    ln -sf $SRC_PATH/config.d/database_replicated.xml $DEST_SERVER_PATH/config.d/
    rm /etc/clickhouse-server/config.d/zookeeper.xml
    rm /etc/clickhouse-server/config.d/keeper_port.xml

    # There is a bug in config reloading, so we cannot override macros using --macros.replica r2
    # And we have to copy configs...
    mkdir -p /etc/clickhouse-server1
    mkdir -p /etc/clickhouse-server2
    chown clickhouse /etc/clickhouse-server1
    chown clickhouse /etc/clickhouse-server2
    chgrp clickhouse /etc/clickhouse-server1
    chgrp clickhouse /etc/clickhouse-server2
    sudo -u clickhouse cp -r /etc/clickhouse-server/* /etc/clickhouse-server1
    sudo -u clickhouse cp -r /etc/clickhouse-server/* /etc/clickhouse-server2

    rm /etc/clickhouse-server1/config.d/macros.xml
    rm /etc/clickhouse-server2/config.d/macros.xml
    sudo -u clickhouse cat /etc/clickhouse-server/config.d/macros.xml | sed "s|<replica>r1</replica>|<replica>r2</replica>|" > /etc/clickhouse-server1/config.d/macros.xml
    sudo -u clickhouse cat /etc/clickhouse-server/config.d/macros.xml | sed "s|<shard>s1</shard>|<shard>s2</shard>|" > /etc/clickhouse-server2/config.d/macros.xml

    rm /etc/clickhouse-server1/config.d/transactions.xml
    rm /etc/clickhouse-server2/config.d/transactions.xml
    sudo -u clickhouse cat /etc/clickhouse-server/config.d/transactions.xml | sed "s|/test/clickhouse/txn|/test/clickhouse/txn1|" > /etc/clickhouse-server1/config.d/transactions.xml
    sudo -u clickhouse cat /etc/clickhouse-server/config.d/transactions.xml | sed "s|/test/clickhouse/txn|/test/clickhouse/txn2|" > /etc/clickhouse-server2/config.d/transactions.xml

    sudo mkdir -p /var/lib/clickhouse1
    sudo mkdir -p /var/lib/clickhouse2
    sudo chown clickhouse /var/lib/clickhouse1
    sudo chown clickhouse /var/lib/clickhouse2
    sudo chgrp clickhouse /var/lib/clickhouse1
    sudo chgrp clickhouse /var/lib/clickhouse2
fi

ln -sf $SRC_PATH/client_config.xml $DEST_CLIENT_PATH/config.xml
