import dataclasses
import traceback

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.praktika.info import Info


@dataclasses.dataclass
class TC:
    prefix: str
    is_sequential: bool  # sequential in every integration job
    comment: str
    # Sequential only under the flaky/targeted `--dist=each` schedule; parallel
    # under the normal `--dist=loadfile` schedule. Set for modules that start one
    # cluster per xdist worker under `--dist=each` and then contend on a shared
    # resource (host memory, a fixed host port, or a global Docker-network lock).
    dist_each_sequential: bool = False


# Tests that are too slow to run under LLVM coverage instrumentation.
# They either timeout (900s per-test or 7200s session) or cause ClickHouse
# to get stuck during shutdown while writing .profraw coverage data.
LLVM_COVERAGE_SKIP_PREFIXES = [
    "test_storage_s3_queue/test_6.py",
    "test_named_collections_encrypted2/",
    "test_multiple_disks/",
    "test_ytsaurus/",
    # Starts 20 server nodes. Under continuous-mode coverage (%c) every node
    # memory-maps its own ~178 MB profile, and the kernel's writeback of the
    # dirty counter pages saturates the disk: 48 s on a plain coverage build,
    # over 2 h under %c (blew the 7800 s sequential backstop).
    "test_backup_restore_on_cluster/test_huge_concurrent_restore.py",
    # Asserts wall-clock timing of reconnects with a 5.5 s margin. The %c
    # writeback load pushed a 967/967-green test over the margin (9.6 s
    # observed vs 8.5 s allowed).
    "test_distributed_respect_user_timeouts/",
    # 30 GROUP BY queries (15 concurrent) fill a 4 GB container: ~50 s on
    # a release build, 500-900 s under coverage, vs. a 900 s test timeout.
    "test_memory_limit/",
]

# Additionally skipped on the per-test coverage build (`WITH_COVERAGE_DEPTH`).
PER_TEST_COVERAGE_SKIP_PREFIXES = [
    # Keeper of this build uses 295-343 MiB right after start, above the 286 MiB
    # `max_memory_usage_soft_limit` of the test, so it refuses every write and the
    # test times out.
    "test_keeper_memory_soft_limit/",
]

TEST_CONFIGS = [
    TC(
        "test_dns_cache/",
        False,
        "fixed IPv6 addresses; concurrent --dist=each clusters serialize on the "
        "global /tmp/docker_net.lock and blow the 10-min acquire budget",
        dist_each_sequential=True,
    ),
    TC("test_global_overcommit_tracker/", False, "memory overcommit test; isolated to its own ClickHouse instance"),
    TC(
        "test_profile_max_sessions_for_user/",
        False,
        "uses fixed internal ports (gRPC/MySQL/PostgreSQL) within isolated Docker container",
    ),
    TC("test_random_inserts/", False, "standard replicated inserts test; cluster is fully isolated"),
    TC("test_server_overload/", True, "uses taskset to pin ClickHouse to specific CPU cores; sensitive to concurrent CPU load"),
    TC(
        "test_keeper_snapshot_chunked_transfer/",
        False,
        "18-node Keeper+S3 cluster; concurrent --dist=each copies OOM the ASAN runner",
        dist_each_sequential=True,
    ),
    TC("test_storage_kafka/", False, "each cluster has its own Kafka container and Docker network"),
    TC("test_storage_rabbitmq/", False, "each cluster has its own RabbitMQ container; tests use unique exchange/db names"),
    TC("test_storage_kerberized_kafka/", False, "each cluster has its own Kafka container and Docker network"),
    TC(
        "test_backup_restore_on_cluster/test_concurrency.py",
        False,
        "10-node cluster; fully isolated per test module",
    ),
    TC(
        "test_backup_restore_on_cluster/test_huge_concurrent_restore.py",
        True,
        "20-node cluster; under ASan its concurrent startup saturates the host and overloads Keeper (KEEPER_EXCEPTION on ON CLUSTER queries), timing out co-scheduled tests",
    ),
    TC("test_storage_iceberg_no_spark/", False, "minio/azurite per cluster; fully isolated"),
    TC("test_storage_iceberg_with_spark_cache/", False, "package-scoped Spark session; each xdist worker gets its own instance"),
    TC("test_storage_iceberg_concurrent/", False, "package-scoped Spark session; each xdist worker gets its own instance"),
    TC(
        "test_storage_delta/test_azure_cluster.py",
        True,
        "pins azurite to fixed host port 10000 (emulator mode); concurrent --dist=each workers collide on bind",
    ),
    TC(
        "test_storage_iceberg_interoperability_azure/",
        True,
        "pins azurite to fixed host port 10000 (Spark emulator mode); concurrent --dist=each workers collide on bind",
    ),
    TC(
        "test_storage_delta/test.py",
        False,
        "starts a Spark JVM + multi-node ClickHouse cluster per module fixture",
        dist_each_sequential=True,
    ),
    TC(
        "test_storage_delta/test_cdf.py",
        False,
        "starts a Spark JVM + multi-node ClickHouse cluster per module fixture",
        dist_each_sequential=True,
    ),
    TC(
        "test_storage_delta_disks/test.py",
        False,
        "starts a Spark JVM + multi-node ClickHouse cluster per module fixture",
        dist_each_sequential=True,
    ),
]


def force_heavy_modules_sequential(
    parallel_test_modules: list[str],
    sequential_test_modules: list[str],
) -> tuple[list[str], list[str]]:
    """Move TEST_CONFIGS `dist_each_sequential` modules from the parallel to the
    sequential bucket, preserving order.

    Called only on the flaky/targeted path, whose parallel bucket runs with
    `--dist=each` (every worker runs every parallel module at once). These
    modules start one cluster per worker there and exhaust memory; the
    sequential bucket runs `-n 1` (one cluster at a time, looped >=3x), which
    keeps the flakiness signal without the concurrent OOM. Normal runs use
    `--dist=loadfile` (one file -> one worker -> one cluster) and never call this.
    """
    prefixes = [tc.prefix for tc in TEST_CONFIGS if tc.dist_each_sequential]
    forced = [
        m
        for m in parallel_test_modules
        if any(m.startswith(p) for p in prefixes)
    ]
    if not forced:
        return parallel_test_modules, sequential_test_modules
    new_parallel = [m for m in parallel_test_modules if m not in forced]
    new_sequential = sequential_test_modules + forced
    return new_parallel, new_sequential


IMAGES_ENV = {
    "clickhouse/dotnet-client": "DOCKER_DOTNET_CLIENT_TAG",
    "clickhouse/integration-helper": "DOCKER_HELPER_TAG",
    "clickhouse/integration-test": "DOCKER_BASE_TAG",
    "clickhouse/kerberos-kdc": "DOCKER_KERBEROS_KDC_TAG",
    "clickhouse/test-mysql80": "DOCKER_TEST_MYSQL80_TAG",
    "clickhouse/test-mysql57": "DOCKER_TEST_MYSQL57_TAG",
    "clickhouse/mysql-golang-client": "DOCKER_MYSQL_GOLANG_CLIENT_TAG",
    "clickhouse/mysql-java-client": "DOCKER_MYSQL_JAVA_CLIENT_TAG",
    "clickhouse/mysql-js-client": "DOCKER_MYSQL_JS_CLIENT_TAG",
    "clickhouse/wasm-builder": "DOCKER_WASM_BUILDER_TAG",
    "clickhouse/arrowflight-server-test": "DOCKER_ARROWFLIGHT_SERVER_TAG",
    "clickhouse/mysql-php-client": "DOCKER_MYSQL_PHP_CLIENT_TAG",
    "clickhouse/nginx-dav": "DOCKER_NGINX_DAV_TAG",
    "clickhouse/postgresql-java-client": "DOCKER_POSTGRESQL_JAVA_CLIENT_TAG",
    "clickhouse/python-bottle": "DOCKER_PYTHON_BOTTLE_TAG",
    "clickhouse/integration-test-with-unity-catalog": "DOCKER_BASE_WITH_UNITY_CATALOG_TAG",
    "clickhouse/integration-test-with-hms": "DOCKER_BASE_WITH_HMS_TAG",
    "clickhouse/mysql_dotnet_client": "DOCKER_MYSQL_DOTNET_CLIENT_TAG",
    "clickhouse/s3-proxy": "DOCKER_S3_PROXY_TAG",
}


# Measured test suite durations, used by get_optimal_test_batch to balance shards by duration.
# Regenerate periodically (it drifts as tests change) with the query below on play.clickhouse.com.
# Suites without an entry get weight 0 and are only round-robin distributed, so keep the floor
# low: the more wall-clock mass the table covers, the better the packer balances. The path filter
# drops functional-test rows that share the integration check name.
"""
WITH per_run_suite AS (
    SELECT
        splitByString('::', test_name)[1] AS test_suite,
        check_start_time,
        sum(test_duration_ms) AS suite_duration_ms
    FROM checks
    WHERE check_name LIKE 'Integration tests (amd_asan_ubsan%'
      AND check_start_time > now() - INTERVAL 14 DAYS
      AND test_duration_ms != 0
      AND head_ref = 'master'
    GROUP BY
        test_suite,
        check_start_time
)

SELECT
    test_suite,
    round(median(suite_duration_ms)) AS dur
FROM per_run_suite
WHERE test_suite != ''
  AND match(test_suite, '^test_[^/]+/.*\\.py$')
GROUP BY test_suite
HAVING dur > 1000
ORDER BY dur DESC, test_suite ASC;
"""

RAW_TEST_DURATIONS = """
test_max_bytes_ratio_before_external_order_group_by_for_server/test.py	1628263
test_storage_s3_queue/test_6.py	1614333
test_storage_delta/test.py	1489055
test_storage_kafka/test_batch_fast.py	1488990
test_database_replicated_settings/test.py	1160550
test_replicated_database/test.py	1131399
test_storage_nats/test_nats_core.py	1020584
test_distributed_ddl/test.py	1020376
test_storage_rabbitmq/test.py	1012444
test_multiple_disks/test.py	1002620
test_backup_restore_s3/test.py	976326
test_backup_restore_new/test.py	947851
test_keeper_session/test.py	933840
test_dictionaries_all_layouts_separate_sources/test_mongo.py	931794
test_storage_s3/test.py	911624
test_database_delta/test.py	872315
test_dictionaries_redis/test.py	849195
test_storage_s3_queue/test_5.py	843995
test_distributed_load_balancing/test.py	826051
test_refreshable_mat_view/test.py	804819
test_postgresql_replica_database_engine/test_3.py	760845
test_storage_s3_queue/test_system_stop.py	709968
test_storage_s3_queue/test_2.py	689229
test_restore_db_replica/test.py	664187
test_storage_s3_queue/test_0.py	657374
test_storage_nats/test_nats_jet_stream.py	631476
test_storage_azure_blob_storage/test.py	625662
test_storage_kafka/test_system_stop.py	610374
test_dictionaries_all_layouts_separate_sources/test_clickhouse_remote.py	598401
test_dictionaries_all_layouts_separate_sources/test_clickhouse_local.py	597595
test_dictionaries_all_layouts_separate_sources/test_mysql.py	591898
test_parallel_replicas_insert_select/test.py	587148
test_dictionaries_all_layouts_separate_sources/test_https.py	584129
test_dictionaries_all_layouts_separate_sources/test_http.py	583981
test_refreshable_mat_view_replicated/test.py	579623
test_backup_restore_on_cluster/test_concurrency.py	576987
test_ttl_move/test.py	569136
test_refreshable_mv/test.py	550189
test_async_load_databases/test.py	544682
test_unknown_config_option/test.py	517738
test_http_handlers_config/test.py	516595
test_backup_restore_on_cluster/test.py	514530
test_postgresql_replica_database_engine/test_1.py	513454
test_storage_iceberg_with_spark/test_minmax_pruning.py	511130
test_mask_sensitive_info/test.py	502645
test_dns_cache/test.py	497871
test_storage_nats/test_system_stop.py	477744
test_storage_s3_queue/test_1.py	473292
test_s3_cluster_restart/test.py	473236
test_database_iceberg/test.py	472159
test_distributed_directory_monitor_split_batch_on_failure/test.py	455790
test_storage_rabbitmq/test_system_stop.py	454448
test_named_collections/test.py	447279
test_storage_kafka/test_partition_affinity.py	447080
test_storage_hdfs/test.py	429265
test_storage_iceberg_with_spark/test_cluster_table_function.py	418522
test_concurrent_ttl_merges/test.py	412022
test_storage_s3_queue/test_3.py	409832
test_checking_s3_blobs_paranoid/test.py	404373
test_azure_403_handling/test.py	389156
test_parallel_replicas_over_distributed/test.py	388084
test_merge_tree_s3/test.py	383917
test_crash_log/test.py	379436
test_text_index_upgrade/test.py	377946
test_insert_distributed_async_send/test.py	374686
test_lost_part_during_startup/test.py	374483
test_mysql_database_engine/test.py	372242
test_scheduler_io/test.py	368918
test_filesystem_cache/test.py	364396
test_executable_table_function/test.py	363867
test_ttl_replicated/test.py	360470
test_storage_iceberg_schema_evolution/test_evolved_schema_simple.py	357994
test_cluster_discovery/test.py	356468
test_prometheus_protocols/test_evaluation.py	342552
test_storage_kafka/test_batch_slow_4.py	339967
test_embedded_ca_certificates/test.py	338003
test_named_collections_encrypted2/test.py	335648
test_drop_is_lock_free/test.py	332446
test_kafka_bad_messages/test.py	327514
test_grant_and_revoke/test_with_table_engine_grant.py	326356
test_storage_iceberg_with_spark/test_writes.py	325976
test_filesystem_split_cache/test.py	325658
test_mysql_protocol/test.py	325439
test_dictionaries_dependency/test.py	321824
test_dictionaries_ddl/test.py	318305
test_database_glue/test.py	317024
test_ytsaurus/test_tables.py	310665
test_postgresql_replica_database_engine/test_2.py	303897
test_mysql57_database_engine/test.py	302609
test_lost_part/test.py	296658
test_row_policy/test.py	296468
test_storage_iceberg_with_spark/test_expire_snapshots.py	291336
test_parallel_replicas_custom_key_failover/test.py	289135
test_keeper_snapshot_chunked_transfer/test.py	289120
test_create_as_select_on_cluster_distributed/test.py	288948
test_rename_column/test.py	283768
test_ai_functions/test.py	281822
test_postgresql_database_engine/test.py	278853
test_storage_iceberg_with_spark/test_system_iceberg_metadata.py	276765
test_storage_kafka/test_batch_slow_5.py	276494
test_dictionaries_all_layouts_separate_sources/test_file.py	276024
test_reload_ca_certificate/test.py	272441
test_backup_restore_new/test_cancel_backup.py	268946
test_storage_kafka/test_multi_consumer_quota.py	268571
test_parallel_replicas_invisible_parts/test.py	267444
test_group_by_top_k_distributed/test.py	262445
test_exclude_data_from_backup/test.py	261821
test_storage_iceberg_with_spark/test_partition_pruning.py	261750
test_storage_kafka/test_compression_codec.py	260604
test_parallel_replicas_custom_key_load_balancing/test.py	260007
test_storage_kafka/test_batch_slow_1.py	259726
test_backward_compatibility/test_aggregate_function_state.py	256242
test_postpone_failed_tasks/test.py	255033
test_table_db_num_limit/test.py	253749
test_statistics_cache/test.py	251640
test_create_handler/test.py	251427
test_s3_plain_rewritable/test.py	249399
test_storage_iceberg_with_spark/test_partition_pruning_with_functions.py	248310
test_storage_iceberg_with_spark/test_position_deletes.py	242942
test_storage_iceberg_schema_evolution/test_tuple_evolved_simple.py	240449
test_refreshable_mv_no_multi_read/test.py	239684
test_storage_kafka/test_batch_slow_6.py	239455
test_cleanup_dir_after_bad_zk_conn/test.py	236682
test_backup_restore_on_cluster/test_cancel_backup.py	235282
test_hedged_requests/test.py	234884
test_distributed_inter_server_secret/test.py	233522
test_polymorphic_parts/test.py	233325
test_postgresql_replica_database_engine/test_0.py	233109
test_keeper_internal_secure/test.py	229164
test_storage_bigquery/test.py	228918
test_corrupted_part_files/test.py	227431
test_storage_postgresql/test.py	223262
test_keeper_ttl_nodes/test.py	222949
test_s3_cluster/test.py	222834
test_storage_url/test.py	221013
test_broken_projections/test.py	215014
test_storage_iceberg_with_spark/test_remove_orphan_files.py	213585
test_storage_kafka/test_batch_slow_0.py	213393
test_storage_s3_queue/test_4.py	212186
test_parallel_replicas_custom_key/test.py	212179
test_throttling/test.py	211729
test_implicit_index_upgrade/test.py	211657
test_dictionaries_all_layouts_separate_sources/test_executable_hashed.py	211299
test_backward_compatibility/test_pr_protocol_with_stream_id.py	210790
test_allow_feature_tier/test.py	210066
test_storage_s3_queue/test_flush.py	208770
test_backward_compatibility/test_convert_ordinary.py	208310
test_default_compression_codec/test.py	204722
test_storage_iceberg_schema_evolution/test_array_evolved_nested.py	202433
test_scheduler_memory/test.py	200401
test_insert_into_distributed/test.py	200163
test_parallel_replicas_distributed_skip_shards/test.py	198797
test_insert_distributed_load_balancing/test.py	198778
test_delayed_replica_failover/test.py	194211
test_storage_mysql/test.py	193926
test_storage_iceberg_no_spark/test_writes_statistics_by_minmax_pruning.py	192786
test_dictionaries_all_layouts_separate_sources/test_mongo_uri.py	190999
test_replicated_users/test.py	190746
test_string_aggregation_compatibility/test.py	189686
test_storage_s3/test_sts.py	189308
test_storage_azure_blob_storage/test_cluster.py	187860
test_prometheus_protocols/test_compliance.py	187488
test_jbod_balancer/test.py	186598
test_keeper_container_nodes/test.py	185616
test_storage_mongodb/test.py	185543
test_storage_kerberized_kafka/test.py	184301
test_hive_query/test.py	182796
test_database_cluster/test.py	180460
test_cross_replication/test.py	177729
test_replicated_mutations/test.py	177385
test_disk_over_web_server/test.py	176916
test_file_cluster/test.py	176822
test_distributed_ddl_on_cross_replication/test.py	174762
test_s3_cluster_insert_select/test.py	174721
test_ytsaurus/test_dictionaries.py	173609
test_quorum_inserts/test.py	173100
test_executable_udf_async_metrics/test.py	172497
test_shutdown_cancels_merges/test.py	171259
test_parallel_replicas_protocol/test.py	170223
test_parallel_replicas_alias_columns/test.py	168498
test_s3_plain_rewritable_rotate_tables/test.py	167718
test_stop_insert_when_disk_close_to_full/test.py	167589
test_storage_kafka/test_batch_slow_2.py	167256
test_distributed_frozen_replica/test.py	166610
test_system_logs/test_system_logs.py	166054
test_secure_socket/test.py	165075
test_storage_kafka/test_kafka_zone_awareness.py	164674
test_database_backup/test.py	164621
test_merge_tree_azure_blob_storage/test.py	163824
test_distributed_over_distributed/test.py	163379
test_opentelemetry_trace_view/test.py	163328
test_dictionaries_all_layouts_separate_sources/test_executable_cache.py	163286
test_plain_rewritable_backward_compatibility/test.py	162304
test_max_rows_to_read_leaf_with_view/test.py	161261
test_insert_into_distributed_through_materialized_view/test.py	160959
test_truncate_database/test_distributed.py	159500
test_groupBitmapAnd_on_distributed/test_groupBitmapAndState_on_distributed_table.py	158646
test_merge_tree_hdfs/test.py	158398
test_keeper_two_nodes_cluster/test.py	158210
test_dictionaries_update_and_reload/test.py	157004
test_interserver_dns_retires/test.py	155790
test_prometheus_protocols/test_different_table_engines.py	155736
test_groupBitmapAnd_on_distributed/test.py	155579
test_drop_database_replica/test.py	155341
test_dictionary_ddl_on_cluster/test.py	154952
test_distributed_storage_configuration/test.py	153730
test_encrypted_disk/test.py	152490
test_distributed_default_database/test.py	152153
test_distributed_index_analysis/test.py	150082
test_unknown_column_dist_table_with_alias/test.py	148585
test_replicated_fetches_bandwidth/test.py	148274
test_distributed_config/test.py	148240
test_manipulate_statistics/test.py	147985
test_mutations_with_tampered_parts/test.py	146926
test_restore_replica/test.py	146079
test_transactions/test.py	144992
test_storage_iceberg_disks/test.py	144076
test_keeper_zookeeper_converter/test.py	143986
test_storage_iceberg_with_spark/test_writes_mutate_delete.py	141826
test_log_query_probability/test.py	140958
test_storage_iceberg_with_spark/test_manifest_compaction.py	138124
test_system_clusters_actual_information/test.py	138011
test_keeper_map/test.py	137894
test_cluster_discovery/test_auxiliary_keeper.py	137765
test_database_unity_v2/test.py	137639
test_s3_aws_sdk_has_slightly_unreliable_behaviour/test.py	137573
test_kafka_bad_messages/test_mv_target_missing.py	136524
test_default_session_user/test.py	135877
test_refreshable_mv_skip_old_temp_table_ddls/test.py	135108
test_system_logs_recreate/test.py	134040
test_attach_without_fetching/test.py	133453
test_http_handlers_config_error/test.py	131650
test_transposed_metric_log/test.py	131024
test_quota/test.py	129964
test_ddl_worker_replicas/test.py	128629
test_storage_kafka/test_keeper_session_loss_direct_read.py	127500
test_postgresql_ssl/test.py	125256
test_distributed_ddl_parallel/test.py	122319
test_recompression_ttl/test.py	122189
test_distributed_plan_replicated_merge_tree/test.py	121940
test_recovery_replica/test.py	121174
test_merges_memory_limit/test.py	121135
test_storage_kafka/test_produce_http_interface.py	119780
test_disk_access_storage/test.py	119592
test_format_schema_source/test.py	119506
test_scheduler_query/test.py	119172
test_zookeeper_config/test_secure.py	116864
test_storage_iceberg_schema_evolution/test_tuple_evolved_nested.py	116335
test_storage_iceberg_with_spark/test_writes_create_partitioned_table.py	115734
test_mutations_with_merge_tree/test.py	115691
test_storage_kafka/test_zookeeper_locks.py	115125
test_disk_configuration/test.py	114894
test_distributed_plan_dictionary_fallback/test.py	114780
test_version_update_after_mutation/test.py	114456
test_named_collections_encrypted2/test_integr.py	114211
test_paimon_incremental_read/test.py	113612
test_storage_iceberg_with_spark/test_row_lineage_pruning.py	112976
test_allowed_url_from_config/test.py	112932
test_reloading_storage_configuration/test.py	112330
test_partition/test.py	111274
test_database_remote/test.py	109884
test_host_regexp_multiple_ptr_records/test.py	109784
test_system_merges/test.py	109501
test_arrowflight_interface/test_type_matrix.py	109495
test_storage_s3_queue/test_parallel_inserts.py	109294
test_backup_restore_on_cluster_with_checksum_data_file_name/test.py	109138
test_create_union_system_log_tables/test.py	107963
test_keeper_snapshot_chunked_transfer/test_concurrent.py	107214
test_backup_restore_azure_blob_storage/test.py	107163
test_async_insert_pool_saturation/test.py	106999
test_migration_deduplication_hash/test.py	106460
test_postgresql_protocol/test.py	104926
test_replicated_user_defined_functions/test.py	104720
test_alter_settings_or_comment_on_cluster/test.py	104306
test_alter_moving_garbage/test.py	104113
test_executable_dictionary/test.py	103801
test_executable_user_defined_function/test.py	103648
test_hedged_requests_parallel/test.py	103223
test_replicated_merge_tree_compatibility/test.py	102448
test_dictionaries_postgresql/test.py	102042
test_scheduler_cpu/test.py	101605
test_MemoryTracking/test.py	101322
test_s3_table_functions/test.py	100968
test_database_disk_setting/test.py	99872
test_storage_iceberg_no_spark/test_drop_partition_transforms.py	99453
test_dictionary_lazy_load/test.py	99338
test_backup_restore_keeper_map/test.py	99120
test_storage_iceberg_with_spark/test_data_manifest_decode_concurrency.py	98549
test_storage_iceberg_with_spark/test_writes_create_table.py	97792
test_merge_tree_load_parts/test.py	97145
test_dictionaries_mysql/test.py	97024
test_modify_engine_on_restart/test_ordinary.py	96652
test_backward_compatibility/test_aggregate_function_state_contingency_functions.py	96638
test_drop_replica_with_auxiliary_zookeepers/test.py	96024
test_settings_profile/test.py	95996
test_restore_external_engines/test.py	93886
test_storage_iceberg_with_spark/test_query_condition_cache.py	93526
test_globs_in_filepath/test.py	93223
test_executable_udf_profile_events/test.py	92756
test_allow_non_metadata_alters_on_cluster/test.py	92237
test_keeper_back_to_back/test.py	92033
test_lightweight_update_on_cluster_replicated/test.py	91630
test_storage_iceberg_no_spark/test_iceberg_inverted_manifest_bounds.py	91596
test_storage_iceberg_with_spark/test_metadata_file_format_with_uuid.py	91394
test_cluster_discovery/test_dynamic_clusters.py	91122
test_storage_iceberg_with_spark/test_metadata_file_selection.py	90861
test_inserts_with_keeper_retries/test.py	90712
test_backup_source_grants/test.py	90629
test_check_table/test.py	90346
test_keeper_remove_rejoin_leader/test.py	90096
test_rocksdb_options/test.py	89946
test_distributed_ddl/test_replicated_alter.py	89780
test_azure_blob_storage_plain_rewritable/test.py	89424
test_storage_iceberg_schema_evolution/test_array_evolved_with_struct.py	89395
test_keeper_incorrect_config/test.py	89150
test_backups_from_disk/test.py	89012
test_keeper_reconfig_replace_leader_in_one_command/test.py	88780
test_random_inserts/test.py	88570
test_scheduler_cpu_preemptive/test.py	88562
test_storage_iceberg_with_spark/test_schema_evolution_with_time_travel.py	88296
test_storage_nats/test_nats_credentials_rotation.py	88178
test_backward_compatibility/test_aggregate_function_state_tuple_return_type.py	87430
test_system_metrics/test.py	87304
test_server_reload/test.py	86972
test_storage_iceberg_with_spark/test_writes_mutate_update.py	86875
test_restore_replica_metadata_version/test.py	86764
test_keeper_password/test.py	86485
test_user_valid_until/test.py	86342
test_system_start_stop_listen/test.py	85944
test_remote_blobs_naming/test_backward_compatibility.py	85382
test_modify_engine_on_restart/test.py	85237
test_backup_restore_on_cluster/test_disallow_concurrency.py	85046
test_backward_compatibility/test_block_marshalling.py	84936
test_zookeeper_send_window_broken_promise/test.py	84611
test_jbod_ha/test.py	84466
test_keeper_opentelemetry_tracing/test.py	84292
test_access_control_with_custom_setup/test.py	83938
test_storage_policies/test.py	83842
test_replication_credentials/test.py	83704
test_s3_table_function_with_http_proxy/test.py	83038
test_ddl_create_then_alter_offline_replica/test.py	83024
test_s3_table_function_with_https_proxy/test.py	82804
test_keeper_three_nodes_two_alive/test.py	82746
test_storage_hudi/test.py	82516
test_system_flush_logs/test.py	82485
test_consistant_parts_after_move_partition/test.py	82250
test_prometheus_protocols/test_insert_select.py	82238
test_s3_credentials_hardening/test.py	81908
test_refreshable_mv_watch_fault/test.py	81793
test_dictionaries_dependency_xml/test.py	81728
test_query_runner/test.py	81564
test_storage_delta_disks/test.py	81540
test_distributed_format/test.py	81251
test_replicated_database_interserver_host/test.py	81152
test_clickhouse_server_wait_server_pool/test.py	81150
test_store_cleanup/test.py	80556
test_rmv_access_denied_on_rename_race/test.py	80434
test_database_catalog_shutdown_system_logs/test.py	80414
test_storage_iceberg_with_spark/test_schema_inference.py	80033
test_tmp_policy/test.py	79657
test_set_engine_row_policy_serialized_plan/test.py	79408
test_always_fetch_merged/test.py	79394
test_external_database_marker_fsync/test.py	79236
test_https_replication/test.py	78849
test_azure_blob_storage_native_copy/test.py	78737
test_constraint_subquery_legacy_metadata/test.py	78206
test_client_auto_secure_port/test.py	77075
test_mark_cache_profile_events/test.py	76680
test_file_schema_inference_cache/test.py	76644
test_user_memory_tracker_log_drift/test.py	76429
test_keeper_disks/test.py	76053
test_keeper_ttl_nodes/test_disabled.py	75924
test_skip_empty_columns/test.py	75907
test_log_family_hdfs/test.py	75685
test_keeper_map_retries/test.py	74940
test_storage_iceberg_schema_evolution/test_evolved_schema_complex.py	74539
test_replicated_merge_tree_encryption_codec/test.py	73749
test_index_filename_upgrade/test.py	73586
test_storage_kafka/test_avro_schema_registry.py	73261
test_multi_access_storage_role_management/test.py	73167
test_keeper_container_nodes/test_disabled.py	73047
test_storage_redis/test.py	72544
test_keeper_mntr_pressure/test.py	72506
test_keeper_as_server/test.py	72441
test_storage_kafka_sasl/test.py	71980
test_modify_engine_on_restart/test_table_readonly.py	71879
test_http_failover/test.py	71666
test_replicated_table_attach/test.py	71581
test_sparsity_exact_num_defaults_compat/test.py	71221
test_replicated_database_recover_digest_mismatch/test.py	70600
test_statistics_non_physical_upgrade/test.py	70600
test_replicated_database_cluster_groups/test.py	70416
test_storage_s3_queue/test_sts_smoke.py	70373
test_storage_iceberg_with_spark/test_explicit_metadata_file.py	70362
test_server_overload/test.py	69992
test_merge_tree_s3_failover/test.py	69430
test_sharding_key_from_default_column/test.py	69416
test_lightweight_updates/test.py	68606
test_projection_unavailable_at_startup/test.py	68322
test_keeper_force_recovery/test.py	67680
test_storage_iceberg_with_spark/test_iceberg_snapshot_reads.py	67439
test_packed_io/test.py	67284
test_replicated_merge_tree_wait_on_shutdown/test.py	67041
test_database_hdfs_read_grant/test.py	66986
test_lightweight_updates_compatibility/test.py	66630
test_http_connection_socket_buffer_settings/test.py	66457
test_redirect_url_storage/test.py	66409
test_parallel_replicas_snapshot_from_initiator/test.py	65680
test_named_collections_if_exists_on_cluster/test.py	65678
test_graphite_merge_tree/test.py	65593
test_storage_kafka/test_poll_timeout_after_assignment.py	64830
test_keeper_shutdown_connections/test.py	64625
test_keeper_persistent_watches/test.py	64467
test_alter_on_mixed_type_cluster/test.py	64446
test_postgresql_remote_host_filter/test.py	64293
test_storage_iceberg_with_trino/test.py	64293
test_storage_iceberg_no_spark/test_incremental_refreshable_mv_iceberg.py	64138
test_storage_iceberg_with_spark/test_read_in_order.py	63791
test_server_metadata_files/test.py	63194
test_attach_tampered_detached_parts/test.py	63170
test_intersect_or_except_kill_query/test.py	63077
test_async_insert_memory/test.py	62878
test_backup_restore_on_cluster/test_huge_concurrent_restore.py	62626
test_graphite_merge_tree_typed/test.py	62297
test_auth_method_grants_deferred_expiry/test.py	62033
test_backward_compatibility/test_bucketed_map_order.py	62024
test_dictionaries_replace/test.py	61529
test_executable_pool_udf_profile_events/test.py	61379
test_grpc_protocol/test.py	61146
test_settings_constraints/test.py	60820
test_storage_s3_queue/test_foreign_processing_state.py	60240
test_storage_kafka/test_intent_sizes.py	60181
test_iceberg_disk_client_replacement/test.py	60084
test_ssh/test.py	59786
test_keeper_reconfig_remove_many/test.py	59652
test_replicated_merge_tree_s3/test.py	59427
test_group_array_element_size/test.py	59324
test_keeper_max_append_byte_size/test.py	59082
test_background_operations_config/test.py	59039
test_phantom_parts_in_mutations/test.py	58968
test_limited_replicated_fetches/test.py	58856
test_storage_iceberg_with_spark/test_delete_files.py	58477
test_backup_restore_new/test_shutdown_wait_backup.py	58443
test_table_function_mongodb/test.py	58428
test_no_merges_volume_ttl/test.py	58357
test_zookeeper_config_load_balancing/test.py	58204
test_replace_partition/test.py	58169
test_keeper_auth/test.py	57981
test_distributed_ddl_password/test.py	57763
test_backup_restore_workload_entities/test.py	57568
test_totp_auth/test_totp.py	57492
test_replicated_merge_tree_encrypted_disk/test.py	57432
test_unique_key_sst/test.py	57254
test_system_detached_tables/test.py	57226
test_send_request_to_leader_replica/test.py	57098
test_backup_restore_on_cluster/test_two_shards_two_replicas.py	57055
test_storage_iceberg_concurrent/test_concurrent_reads.py	56434
test_storage_iceberg_with_spark/test_row_lineage.py	56407
test_async_insert_adaptive_busy_timeout/test.py	56298
test_attach_partition_using_copy/test.py	56215
test_access_control_on_cluster/test.py	56164
test_storage_iceberg_interoperability_azure/test_interoperability.py	56060
test_attach_broken_detached_part/test.py	56018
test_storage_iceberg_with_spark/test_writes_schema_evolution.py	55676
test_arrowflight_interface/test_sql_server.py	55657
test_keeper_4lw_reconfiguration_retry/test.py	55651
test_https_replication/test_change_ip.py	55534
test_storage_iceberg_with_spark_cache/test_metadata_cache.py	55330
test_version_update/test.py	55244
test_ddl_on_cluster_stop_waiting_for_offline_hosts/test.py	55076
test_system_ddl_worker_queue/test.py	55054
test_keeper_nodes_remove/test.py	54907
test_keeper_snapshot_small_distance/test.py	54904
test_storage_iceberg_schema_evolution/test_array_map_evolved_with_struct.py	54748
test_keeper_nodes_add/test.py	54348
test_storage_nats/test_nats_jetstream_credentials_rotation.py	54136
test_warning_broken_tables/test.py	54042
test_named_collections_encrypted/test.py	54002
test_old_parts_finally_removed/test.py	53830
test_mysql_kill_query/test.py	53760
test_storage_numbers/test.py	53676
test_arrowflight_interface/test.py	53620
test_s3_list_objects_empty_page/test.py	53498
test_rocksdb_backup_restore/test.py	53402
test_nullable_tuple_subcolumns/test.py	53355
test_keeper_block_acl/test.py	53044
test_database_iceberg_lakekeeper_catalog/test.py	52960
test_temporary_data_in_cache/test.py	52718
test_quorum_inserts_parallel/test.py	52564
test_read_only_table/test.py	52150
test_memory_limit_observer/test.py	52146
test_replicated_merge_tree_with_auxiliary_zookeepers/test.py	51729
test_disabled_access_control_improvements/test_row_policy.py	51318
test_mutations_in_partitions_of_merge_tree/test.py	51028
test_max_suspicious_broken_parts_replicated/test.py	50932
test_storage_iceberg_schema_evolution/test_struct_with_nested_array_evolved.py	50928
test_storage_iceberg_with_spark/test_drop_partition_whole_manifest.py	50781
test_reload_auxiliary_zookeepers/test.py	49926
test_replicated_access/test.py	49886
test_concurrent_queries_restriction_by_query_kind/test.py	49812
test_consistent_parts_after_clone_replica/test.py	49775
test_storage_alias_replicated/test.py	49731
test_user_directories/test.py	49538
test_modify_engine_on_restart/test_unsafe_name.py	49454
test_database_iceberg_nessie_catalog/test.py	48955
test_database_iceberg_seaweedfs_catalog/test.py	48888
test_config_zk_invalid_yaml_from_include/test.py	48702
test_storage_iceberg_with_spark/test_async_metadata_refresh.py	48701
test_https_s3_table_function_with_http_proxy_no_tunneling/test.py	48663
test_ldap_external_user_directory/test.py	48663
test_storage_kafka/test_schema_registry_skip_bytes.py	48593
test_fetch_partition_should_reset_mutation/test.py	48422
test_encrypted_disk_replication/test.py	48360
test_keeper_readahead/test.py	48353
test_ttl_drop_not_postponed_by_size/test.py	48333
test_matview_union_replicated/test.py	48315
test_storage_iceberg_with_spark/test_bucket_partition_pruning.py	47920
test_fetch_partition_from_auxiliary_zookeeper/test.py	47875
test_non_default_compression/test.py	47713
test_backward_compatibility/test_parallel_replicas_protocol.py	47392
test_backward_compatibility/test_aggregation_with_out_of_order_buckets.py	47304
test_parallel_replicas_increase_error_count/test.py	47160
test_reload_clusters_config/test.py	47062
test_config_substitutions/test.py	47009
test_backup_restore_on_cluster/test_slow_rmt.py	46942
test_disabled_access_control_improvements/test_users_without_row_policies_can_read_rows.py	46756
test_seccomp/test.py	46674
test_keeper_znode_time/test.py	46660
test_settings_constraints_distributed/test.py	46616
test_alter_database_on_cluster/test.py	46592
test_backup_restore/test.py	46386
test_on_cluster_timeouts/test.py	46359
test_fetch_partition_with_outdated_parts/test.py	46258
test_backup_restore_on_cluster_s3_credentials/test.py	46251
test_join_set_family_s3/test.py	45902
test_storage_iceberg_with_spark/test_writes_with_partitioned_table.py	45823
test_race_condition_for_replicated_merge_tree/test.py	45693
test_variant_escaping_merge_tree_compatibility/test.py	45524
test_grant_and_revoke/test_without_table_engine_grant.py	45498
test_replicated_s3_zero_copy_drop_partition/test.py	45131
test_broken_tmp_txn_version_startup/test.py	44997
test_drop_replica/test.py	44738
test_statistics_minmax_upgrade/test.py	44439
test_storage_log_damaged_array/test.py	44353
test_postgresql_kill_query/test.py	44183
test_concurrent_threads_soft_limit/test.py	44160
test_kafka_bad_messages/test_1.py	44151
test_part_uuid/test.py	43992
test_backup_restore_on_cluster/test_different_versions.py	43985
test_s3_disk_connections_soft_limit/test.py	43966
test_arrowflight_storage/test.py	43872
test_zookeeper_config/test.py	43774
test_disks_app_func/test.py	43686
test_keeper_snapshots/test.py	43496
test_distributed_insert_backward_compatibility/test.py	43048
test_zookeeper_connection_log/test.py	42806
test_backward_compatibility/test_adaptive_codec.py	42426
test_keeper_multinode_simple/test.py	41861
test_storage_kafka/test_batch_slow_7.py	41698
test_replicated_fetches_min_part_level/test.py	41666
test_storage_s3/test_invalid_env_credentials.py	41615
test_dictionaries_redis/test_long.py	41614
test_keeper_four_word_command/test.py	41451
test_attach_with_different_projections_or_indices/test.py	41413
test_keeper_commit_readahead_no_reset/test.py	41151
test_startup_scripts/test.py	41141
test_ssl_cert_authentication/test.py	41119
test_force_drop_table/test.py	41042
test_config_decryption/test_wrong_settings.py	40974
test_insert_over_http_query_log/test.py	40920
test_dictionaries_config_reload/test.py	40764
test_part_loading_tree_rollback/test.py	40426
test_profile_max_sessions_for_user/test.py	40381
test_keeper_feature_flags_config/test.py	40274
test_storage_iceberg_with_spark/test_optimize.py	40182
test_keeper_nodes_move/test.py	39809
test_storage_iceberg_with_spark/test_metadata_file_selection_from_version_hint.py	39740
test_zookeeper_config/test_password.py	39740
test_parallel_table_shutdown/test.py	39670
test_wasm_udf_fail_close/test.py	39428
test_executable_user_defined_functions_config_reload/test.py	39372
test_backup_restore_storage_policy/test.py	39291
test_enable_user_name_access_type/test.py	39067
test_async_metrics_key_values_mode/test.py	39062
test_storage_iceberg_interoperability_local/test_interoperability.py	38968
test_parts_delete_zookeeper/test.py	38917
test_modify_engine_on_restart/test_storage_policies.py	38845
test_keeper_session_refuse_stale_server/test.py	38715
test_backward_compatibility/test_ip_types_binary_compatibility.py	38669
test_ddl_worker_stale_task_name/test.py	38640
test_db_ordinary_deprecated_warning/test.py	38620
test_storage_iceberg_schema_evolution/test_map_evolved_nested.py	38511
test_keeper_broken_logs/test.py	38323
test_zookeeper_fallback_session/test.py	38321
test_parallel_replicas_failover/test.py	38308
test_restart_with_unavailable_azure/test.py	38043
test_keeper_profiler/test.py	38016
test_mysql_handshake_timeout/test.py	37714
test_alternative_keeper_config/test.py	37576
test_database_hms/test.py	37432
test_reload_client_certificate/test.py	37340
test_storage_iceberg_no_spark/test_identity_partition_column_projection.py	37265
test_projection_unavailable_replicated_db/test.py	37189
test_database_hms/test_ttransport_exception_reproduction.py	37169
test_undrop_query/test.py	37042
test_force_deduplication/test.py	36997
test_zero_copy_drop_table_with_leftover/test.py	36695
test_keeper_dynamic_settings/test.py	36693
test_prometheus_protocols/test_upgrade_from_prealpha.py	36656
test_storage_iceberg_with_spark/test_explanation.py	36540
test_odbc_interaction/test.py	36421
test_move_partition_to_volume_async/test.py	36397
test_storage_iceberg_no_spark/test_writes_rename_column.py	36379
test_storage_delta/test_azure_cluster.py	36359
test_keeper_max_request_size/test.py	36346
test_ddl_alter_query/test.py	36169
test_keeper_4lw_reconfiguration/test.py	36148
test_storage_delta_shuffles/test.py	36095
test_atomic_drop_table/test.py	36061
test_s3_storage_conf_proxy/test.py	35917
test_reload_zookeeper/test.py	35912
test_storage_iceberg_with_spark/test_dropped_column_in_data_file.py	35789
test_keeper_reconfig_remove/test.py	35716
test_dictionaries_select_all/test.py	35688
test_storage_iceberg_with_spark/test_writes_commit_retry_reuses_manifests.py	35529
test_storage_iceberg_schema_evolution/test_struct_with_nested_map_evolved.py	35497
test_cleanup_after_start/test.py	35462
test_system_logs_comment/test.py	35458
test_keeper_reconfig_replace_leader/test.py	35388
test_dremio_engine/test.py	35360
test_replicated_fetches_timeouts/test.py	35289
test_max_suspicious_broken_parts/test.py	35217
test_attach_partition_with_large_destination/test.py	35162
test_storage_nats/test_nats_tls.py	35142
test_keeper_force_recovery_single_node/test.py	35132
test_select_access_rights/test_from_system_tables.py	34969
test_async_insert_queue_metrics/test.py	34927
test_backup_restore_s3_role_arn/test.py	34801
test_session_log/test.py	34745
test_patch_parts_stale_invalidated_columns/test.py	34636
test_distributed_async_insert_for_node_changes/test.py	34590
test_mutations_with_projection/test.py	34534
test_keeper_log_gap_before_committed/test.py	34293
test_keeper_s3_snapshot/test.py	34045
test_prometheus_endpoint/test.py	33998
test_storage_iceberg_no_spark/test_writes_manifest_list_partition_summary.py	33908
test_modify_engine_on_restart/test_mv.py	33831
test_disk_checker/test.py	33455
test_distributed_respect_user_timeouts/test.py	33200
test_s3_client_refresh/test.py	33152
test_force_restore_data_flag_for_keeper_dataloss/test.py	33114
test_restart_server/test.py	33062
test_mq_remote_host_filter/test.py	33008
test_buffer_flush_on_shutdown/test.py	32972
test_keeper_snapshot_on_exit/test.py	32917
test_storage_delta/test_cdf.py	32689
test_keeper_snapshot_rotation_race/test.py	32571
test_prometheus_protocols/test_async_insert.py	32510
test_rocksdb_read_only/test.py	32364
test_zero_copy_invalidated_system_columns/test.py	32332
test_replication_without_zookeeper/test.py	32266
test_userspace_page_cache/test.py	32246
test_hot_reload_storage_policy/test.py	32168
test_reloading_settings_from_users_xml/test.py	32136
test_suggestions/test.py	31917
test_ddl_worker_non_leader/test.py	31816
test_storage_s3_queue/test_dimensional_metrics.py	31786
test_user_query_log_config_validation/test.py	31709
test_server_startup_and_shutdown_logs/test.py	31644
test_asynchronous_metrics_pk_bytes_fields/test.py	31614
test_search_orphaned_parts/test.py	31427
test_distributed_structure_fetch/test.py	31304
test_modify_engine_on_restart/test_zk_path_exists.py	31292
test_materialize_projections_on_merge/test.py	31273
test_storage_iceberg_with_spark/test_minmax_pruning_with_null.py	31243
test_keeper_persistent_log/test.py	31109
test_arrowflight_storage/test_nullable_struct.py	30903
test_compression_nested_columns/test.py	30864
test_ttl_multilevel_group_by/test.py	30740
test_webassembly_udf/test.py	30689
test_keeper_persistent_log_multinode/test.py	30624
test_paimon_rest_catalog/test.py	30600
test_grpc_arrowflight_listen_hosts/test.py	30573
test_cache_bypass_on_disk_failure/test.py	30560
test_backward_compatibility/test_functions.py	30553
test_broken_detached_part_quarantine/test.py	30524
test_distributed_plan_worker_exchange_port/test.py	30506
test_analyzer_compatibility/test.py	30493
test_disabled_access_control_improvements/test_select_from_system_tables.py	30484
test_access_cache_recompute_coalescing/test.py	30416
test_incremental_refreshable_mv/test.py	30396
test_keeper_reconfig_add/test.py	30394
test_storage_iceberg_with_spark/test_file_stats_logging.py	30369
test_merge_tree_s3_with_cache/test.py	30330
test_dictionaries_wait_for_load/test.py	30314
test_acme_tls/test_single_node.py	30274
test_compressed_marks_restart/test.py	30263
test_keeper_slow_member_backpressure/test.py	30141
test_temporary_data/test.py	30094
test_storage_s3_queue/test_file_iterator_ttl.py	30085
test_storage_iceberg_with_spark/test_lifetime_sources_bug.py	30078
test_concurrent_part_removal_threshold_for_remote_disk/test.py	29994
test_iceberg_manifest_decode_pool/test.py	29994
test_play_reconcile_startup/test.py	29900
test_parallel_replicas_no_replicas/test.py	29841
test_acme_tls/test_multi_node.py	29788
test_storage_iceberg_no_spark/test_writes_multiple_files.py	29747
test_system_queries/test.py	29694
test_disable_insertion_and_mutation/test.py	29691
test_remove_stale_moving_parts/test.py	29599
test_config_substitutions_xml_escape/test.py	29463
test_modify_engine_on_restart/test_args.py	29310
test_storage_iceberg_with_spark/test_writes_partition_transforms_utc.py	29308
test_modify_engine_on_restart/test_unusual_path.py	29285
test_user_defined_object_persistence/test.py	29275
test_ddl_worker_with_loopback_hosts/test.py	29189
test_rabbitmq_malicious_broker/test.py	29172
test_storage_iceberg_with_spark/test_column_names_with_dots.py	29091
test_auth_method_valid_until_stateful_protocols/test.py	29054
test_backup_restore_s3/test_throttling.py	29020
test_storage_iceberg_with_spark/test_format_version_upgrade.py	29008
test_storage_iceberg_schema_evolution/test_nested_containers_evolved.py	29005
test_auth_method_grants_on_cluster/test.py	28998
test_parallel_replicas_all_marks_read/test.py	28974
test_dictionary_asynchronous_metrics/test.py	28972
test_log_family_s3/test.py	28964
test_storage_iceberg_with_spark/test_manifest_list_partition_pruning.py	28896
test_replicated_database_active_node_taken/test.py	28872
test_keeper_azure_s3_plain/test.py	28814
test_shutdown_wait_unfinished_queries/test.py	28798
test_parallel_replicas_skip_inactive_replicas_all_groups/test.py	28785
test_parallel_replicas_skip_inactive_replicas/test.py	28776
test_dictionary_allow_read_expired_keys/test_default_reading.py	28723
test_mongodb_kill_query/test.py	28705
test_permissions_drop_replica/test.py	28679
test_sync_replica_on_cluster/test.py	28653
test_storage_iceberg_no_spark/test_cluster_partition_pruning_reads.py	28618
test_validate_only_initial_alter_query/test_replicated_database.py	28566
test_dictionary_allow_read_expired_keys/test_dict_get.py	28491
test_keeper_unpreprocessed_logs_livelock/test.py	28438
test_match_process_uid_against_data_owner/test.py	28326
test_filesystem_cache_eviction_metrics/test.py	28256
test_insert_into_distributed_sync_async/test.py	28057
test_keeper_raft_cert_reload/test.py	28011
test_sqlite_kill_query/test.py	27986
test_placement_info/test.py	27968
test_reset_ddl_worker/test.py	27954
test_filesystem_layout/test.py	27894
test_experimental_codec_config_default/test.py	27772
test_keeper_follower_metrics/test.py	27772
test_dictionary_allow_read_expired_keys/test_dict_get_or_default.py	27762
test_storage_iceberg_with_spark/test_writes_field_ids_spark_read.py	27720
test_compatibility_merge_tree_settings/test.py	27675
test_external_distinct_disk_limits/test.py	27673
test_covered_by_broken_exists/test.py	27630
test_drop_if_empty/test.py	27538
test_bind_host/test.py	27513
test_storage_iceberg_no_spark/test_writes_multiple_threads.py	27347
test_input_format_parallel_parsing_memory_tracking/test.py	27308
test_system_logs_hostname/test_replicated.py	27301
test_fix_metadata_version/test.py	27137
test_keeper_remove_acl/test.py	26890
test_executable_udf_names_in_system_query_log/test.py	26834
test_part_log_table/test.py	26822
test_detached_parts_metrics/test.py	26808
test_extreme_deduplication/test.py	26676
test_replicated_access/test_invalid_entity.py	26616
test_s3_access_headers/test.py	26545
test_backup_log/test.py	26539
test_replica_is_active/test.py	26349
test_ddl_worker_retry_when_dropping_db_failed/test.py	26248
test_check_table_name_length_2/test.py	26057
test_mutation_fetch_fallback/test.py	26001
test_settings_constraints_distributed_ddl/test.py	25925
test_settings_from_server/test.py	25880
test_topk_alpha_map_compatibility/test.py	25860
test_format_avro_confluent/test.py	25807
test_alter_comment_on_cluster/test.py	25715
test_server_start_and_ip_conversions/test.py	25552
test_disabled_mysql_server/test.py	25397
test_limit_after_until_distributed/test.py	25357
test_keeper_three_nodes_start/test.py	25198
test_storage_iceberg_with_spark_cache/test_filesystem_cache.py	25166
test_parallel_replicas_insert_select_coordinator_reuse/test.py	24863
test_cow_policy/test.py	24852
test_access_for_functions/test.py	24847
test_table_function_redis/test.py	24760
test_distributed_ddl_on_database_cluster/test.py	24650
test_no_password_existing_user/test.py	24616
test_default_database_on_cluster/test.py	24610
test_parallel_replicas_local_replica_forced_inactive/test.py	24522
test_alter_settings_on_cluster/test.py	24482
test_old_versions/test.py	24456
test_keeper_leader_metrics/test.py	24454
test_default_compression_in_mergetree_settings/test.py	24450
test_mutations_hardlinks/test.py	24389
test_optimize_on_insert/test.py	24388
test_azure_blob_storage_listobjects_prefix/test.py	24324
test_external_cluster/test.py	24195
test_totime_legacy_replicated_delete/test.py	24156
test_broken_part_during_merge/test.py	24141
test_storage_iceberg_no_spark/test_local_path_traversal.py	24118
test_zero_copy_lock_leak/test.py	24056
test_log_lz4_streaming/test.py	23958
test_threadpool_readers/test.py	23952
test_keeper_snapshots_multinode/test.py	23931
test_acme_tls/test_no_certificate.py	23807
test_asynchronous_metric_log_table/test.py	23751
test_early_memory_limit_exception/test.py	23654
test_storage_iceberg_with_spark/test_writes_multiple_threads.py	23572
test_profile_events_s3/test.py	23530
test_intersecting_parts/test.py	23497
test_move_partition_to_disk_on_cluster/test.py	23448
test_ddl_config_hostname/test.py	23304
test_keeper_mntr_data_size/test.py	23288
test_storage_iceberg_no_spark/test_expire_snapshots_history.py	23244
test_storage_s3/test_parquet_prewhere.py	23222
test_replicated_database_alter_modify_order_by/test.py	23219
test_peak_memory_usage/test.py	23188
test_storage_iceberg_with_spark/test_writes_complex_type.py	23151
test_max_authentication_methods_per_user/test.py	23116
test_azure_disk_unreachable/test.py	23053
test_external_http_authenticator/test.py	23043
test_keeper_restore_from_snapshot/test_disk_s3.py	23032
test_deduplicated_attached_part_rename/test.py	22974
test_parameterized_view/test.py	22968
test_cluster_discovery/test_password.py	22934
test_recovery_time_metric/test.py	22814
test_truncate_database/test_replicated.py	22805
test_storage_iceberg_with_spark/test_writes_from_zero.py	22686
test_storage_iceberg_schema_evolution/test_full_drop.py	22680
test_paimon_metadata_files_cache/test.py	22658
test_storage_iceberg_with_spark/test_manifest_read_performance.py	22597
test_storage_iceberg_schema_evolution/test_correct_column_mapper_is_chosen.py	22590
test_replicated_database_with_auxiliary_zookeepers/test.py	22556
test_storage_iceberg_with_spark/test_geometry_types.py	22552
test_replicated_merge_tree_thread_schedule_timeouts/test.py	22503
test_http_limits/test_hard_limit.py	22468
test_format_schema_on_server/test.py	22444
test_storage_iceberg_with_spark/test_partition_pruning_with_subquery_set.py	22433
test_aliases_in_default_expr_not_break_table_structure/test.py	22406
test_storage_iceberg_with_spark/test_multiple_iceberg_file.py	22285
test_sql_user_defined_functions_on_cluster/test.py	22241
test_s3_storage_conf_new_proxy/test.py	22236
test_s3_low_cardinality_right_border/test.py	22126
test_sql_roles_for_xml_users/test.py	22101
test_database_hdfs_url_allowlist/test.py	22074
test_prometheus_protocols/test_write_read.py	22027
test_storage_iceberg_with_spark/test_metadata_file_path_security.py	21978
test_replicating_constants/test.py	21893
test_keeper_client_config/test.py	21831
test_prometheus_before_tables/test.py	21819
test_attach_table_from_s3_plain_readonly/test.py	21796
test_point_in_polygon_cache_size/test.py	21787
test_prefer_global_in_and_join/test.py	21709
test_executable_user_defined_function_lifetime_reload/test.py	21700
test_limit_by_transform_kill_query/test.py	21691
test_index_uncompressed_cache_zero_size/test.py	21568
test_parallel_replicas_cluster_shadows_replicated_db/test.py	21513
test_merge_tree_empty_parts/test.py	21486
test_create_query_constraints/test.py	21474
test_runtime_configurable_cache_size/test.py	21448
test_ldap_follow_referrals/test.py	21338
test_storage_iceberg_no_spark/test_cross_bucket_data_files.py	21317
test_zookeeper_session_on_config_reload/test.py	21310
test_auth_method_grants_disabled_method/test.py	21204
test_executable_user_defined_function/test_system_table.py	21167
test_s3_imds/test_simple.py	21162
test_storage_iceberg_with_spark/test_delete_manifest_decode_concurrency.py	21148
test_oom_canary/test.py	21136
test_buffer_chain_shutdown_flush/test.py	21098
test_replicated_merge_tree_replicated_db_ttl/test.py	21089
test_storage_iceberg_no_spark/test_read_in_order_with_pyiceberg.py	21074
test_storage_iceberg_no_spark/test_manifest_length_cache_weight.py	21028
test_system_zookeeper_watches/test.py	20788
test_storage_iceberg_with_spark/test_types.py	20769
test_storage_iceberg_with_spark/test_restart_broken_s3.py	20761
test_replicated_database_system_clusters_log_level/test.py	20706
test_keeper_client/test.py	20624
test_keeper_watches/test.py	20624
test_memory_tracker_large_allocation_trace/test.py	20451
test_async_connect_to_multiple_ips/test.py	20448
test_replicated_detach_table/test.py	20416
test_system_reconnect_zookeeper/test.py	20413
test_insert_deduplication_version_guard/test.py	20378
test_webterminal/test.py	20314
test_keeper_read_during_close/test.py	20283
test_introspection_port/test.py	20223
test_backward_compatibility/test.py	20196
test_backward_compatibility/test_rocksdb_upgrade.py	20188
test_mutations_analyzer_override/test.py	20089
test_create_dictionary_in_startup_script/test.py	20062
test_union_header/test.py	19982
test_attach_table_normalizer/test.py	19962
test_backward_compatibility/test_aggregate_fixed_key.py	19956
test_keeper_rejoin_removed_member_without_restart/test.py	19893
test_jbod_load_balancing/test.py	19853
test_allowed_client_hosts/test.py	19763
test_keeper_catchup_response_queue/test.py	19706
test_insert_distributed_async_extra_dirs/test.py	19695
test_os_thread_nice_value/test.py	19673
test_replica_can_become_leader/test.py	19654
test_role/test_replicated_ddl_current_roles.py	19589
test_geojson_format/test.py	19524
test_replicated_engine_arguments/test.py	19514
test_keeper_empty_multi/test.py	19425
test_limit_materialized_view_count/test.py	19376
test_distributed_async_insert_batch_recovery/test.py	19177
test_s3_style_link/test.py	19173
test_distributed_broken_files_stat/test.py	19163
test_japanese_tokenizer/test.py	19128
test_replicated_merge_tree_config/test.py	19048
test_config_reloader_interval/test.py	19044
test_merge_table_over_distributed/test.py	18903
test_backward_compatibility/test_nullable_sparse_compatibility.py	18890
test_thread_pool_free_size_shutdown/test.py	18890
test_dictionary_allow_read_expired_keys/test_default_string.py	18584
test_alter_update_cast_keep_nullable/test.py	18500
test_storage_iceberg_no_spark/test_writes_with_compression_metadata.py	18445
test_remote_storage_engine_attach/test.py	18424
test_plain_rewr_legacy_layout/test.py	18400
test_storage_iceberg_no_spark/test_iceberg_inverted_delete_bounds.py	18379
test_zero_copy_expand_macros/test.py	18347
test_check_table_name_length/test.py	18147
test_reader_executor_page_cache/test.py	18142
test_config_decryption/test_zk_secure.py	18088
test_storage_s3_queue/test_file_iterator_lost_lock.py	18061
test_async_logger_metrics/test.py	17991
test_timezone_config/test.py	17896
test_prometheus_protocols/test_prometheus_query_log.py	17888
test_storage_gcp_auth/test.py	17862
test_scram_sha256_password_with_replicated_zookeeper_replicator/test.py	17840
test_scheduler_cached_disk/test.py	17829
test_storage_delta/test_imds.py	17717
test_reload_max_table_size_to_drop/test.py	17593
test_max_temporary_data_size_on_disk/test.py	17534
test_replicated_parse_zk_metadata/test.py	17496
test_fetch_memory_usage/test.py	17396
test_git_import/test.py	17281
test_tcp_hello_string_limits/test.py	17261
test_drop_no_local_path/test.py	17248
test_attach_backup_from_s3_plain/test.py	17217
test_arrowflight_interface/test_prepared_statement_ttl.py	17134
test_multiple_authentication_methods/test.py	17114
test_metdata_cache_memory_leak/test.py	17089
test_shard_level_const_function/test.py	17030
test_codec_encrypted/test.py	16952
test_cluster_all_replicas/test.py	16934
test_backward_compatibility/test_memory_bound_aggregation.py	16928
test_shutdown_static_destructor_failure/test.py	16926
test_keeper_availability_zone/test.py	16918
test_password_constraints/test.py	16753
test_disks_app_other_disk_types/test.py	16746
test_storage_iceberg_no_spark/test_drop_partition_concurrent.py	16717
test_aggregation_memory_efficient/test.py	16692
test_log_levels_update/test.py	16690
test_backward_compatibility/test_const_node_optimization.py	16646
test_failed_async_inserts/test.py	16624
test_storage_s3_intelligent_tier/test.py	16593
test_trace_log_build_id/test.py	16584
test_config_not_overriding_args/test.py	16532
test_refreshable_mv_keeper_loss/test.py	16515
test_keeper_java_client/test.py	16491
test_distributed_system_query/test.py	16484
test_config_decryption/test_zk.py	16451
test_storage_iceberg_with_spark/test_manifest_data_path_security.py	16410
test_iceberg_azure_manifest_file_size/test.py	16403
test_keeper_restore_from_snapshot/test.py	16392
test_prometheus_protocols/test_query_cache.py	16392
test_trace_collector_serverwide/test.py	16372
test_azure_workload_identity/test.py	16318
test_keeper_bench_zookeeper/test.py	16318
test_prometheus_protocols/test_query_api.py	16307
test_backup_restore_s3/test_remote_host_filter.py	16271
test_keeper_sanitizer_logs/test.py	16262
test_keeper_slow_connection_log/test.py	16192
test_block_structure_mismatch/test.py	16184
test_kerberos_auth/test.py	16159
test_composable_protocols/test.py	16088
test_reload_certificate/test.py	16039
test_keeper_ipv4_fallback/test.py	15978
test_storage_dict/test.py	15801
test_storage_iceberg_with_spark/test_iceberg_history_summary.py	15792
test_storage_iceberg_with_spark/test_pruning_nullable_bug.py	15734
test_postgresql_protocol/test_kill_query.py	15675
test_storage_iceberg_no_spark/test_writes_nullable_bugs2.py	15653
test_storage_delta/test_sts.py	15595
test_insert_query_profile_events/test.py	15587
test_storage_iceberg_no_spark/test_iceberg_history_large_summary.py	15582
test_user_ip_restrictions/test.py	15580
test_config_decryption/test_wrong_settings_zk.py	15572
test_storage_iceberg_no_spark/test_startup_with_unavailable_bucket.py	15564
test_native_protocol_grouped_writes/test.py	15560
test_build_sets_from_multiple_threads/test.py	15539
test_read_temporary_tables_on_failure/test.py	15538
test_attach_without_checksums/test.py	15522
test_dictionary_custom_settings/test.py	15515
test_shard_names/test.py	15466
test_settings_constraints_config_profiles/test.py	15389
test_sql_security_analyzer_setting/test.py	15387
test_storage_iceberg_with_spark/test_partition_by.py	15307
test_accept_invalid_certificate/test.py	15277
test_projection_rebuild_with_required_columns/test.py	15276
test_http_connection_drain_before_reuse/test.py	15256
test_grpc_protocol_ssl/test.py	15244
test_prometheus_protocols/test_label_values_api.py	15240
test_user_zero_database_access/test_user_zero_database_access.py	15206
test_materialized_view_restart_server/test.py	15194
test_storage_iceberg_no_spark/test_iceberg_history_missing_optional_summary_metrics.py	15187
test_prometheus_protocols/test_metadata_api.py	15004
test_interserver_tables_status_auth/test.py	15000
test_filesystem/test.py	14959
test_merge_tree_load_marks/test.py	14948
test_dot_in_user_name/test.py	14869
test_storage_iceberg_with_spark/test_writes_drop_table.py	14849
test_drop_data/test.py	14736
test_merge_tree_prewarm_cache/test.py	14726
test_jdbc_bridge_hang/test.py	14672
test_tcp_handler_connection_limits/test.py	14654
test_arrowflight_interface/test_ticket_expiration.py	14639
test_zookeeper_info/test.py	14599
test_ttl_to_disk_wrapped_by_cache/test.py	14553
test_concurrent_queries_for_all_users_restriction/test.py	14538
test_config_xml_full/test.py	14520
test_jemalloc_merge_tree_arenas/test.py	14515
test_cache_s3_object_truncation/test.py	14460
test_tlsv1_3/test.py	14349
test_backward_compatibility/test_short_strings_aggregation.py	14348
test_ssh_keys_authentication/test.py	14250
test_mysql_protocol/test_kill_query.py	14223
test_enabling_access_management/test.py	14195
test_storage_iceberg_with_spark/test_writes_field_partitioning.py	14122
test_user_grants_from_config/test.py	14118
test_http_header_limits/test.py	14116
test_prometheus_protocols/test_labels_api.py	14095
test_backward_compatibility/test_normalized_count_comparison.py	14089
test_storage_azure_blob_storage/test_check_after_upload.py	14079
test_storage_iceberg_no_spark/test_writes_with_snappy_compression_metadata.py	14073
test_iceberg_rest_catalog_server/test.py	14072
test_s3_with_https/test.py	14062
test_config_xml_yaml_mix/test.py	14032
test_prometheus_protocols/test_series_api.py	14030
test_disabled_access_control_improvements/test_impersonate_user.py	14014
test_backward_compatibility/test_select_aggregate_alias_column.py	14013
test_storage_iceberg_interoperability_local/test_full_path_scheme.py	13994
test_aggregating_in_order_transform_kill_query/test.py	13954
test_user_query_log_disabled/test.py	13886
test_storage_iceberg_with_spark/test_writes_create_version_hint.py	13854
test_distributed_plan_cancel/test.py	13798
test_keeper_dynamic_log_level/test.py	13791
test_zookeeper_info_number_overflow/test.py	13754
test_text_log_level/test.py	13720
test_users_config_include_from_reload/test.py	13709
test_s3_imds/test_session_token.py	13695
test_storage_url_http_headers/test.py	13664
test_allow_plaintext_and_no_password/test.py	13654
test_s3_storage_class_multipart/test.py	13624
test_keeper_availability_zone_quorum_reads/test.py	13531
test_config_yaml_main/test.py	13527
test_config_yaml_full/test.py	13452
test_custom_settings/test.py	13434
test_naive_bayes_xml_dictionary/test.py	13405
test_merge_tree_settings_constraints/test.py	13394
test_executable_udf_driver_config_reload/test.py	13391
test_skip_local_missing_table/test.py	13380
test_system_reload_async_metrics/test.py	13374
test_paimon_spark_smoke/test.py	13348
test_storage_iceberg_no_spark/test_writes_v3_row_lineage_partitioned.py	13332
test_config_yaml_merge_keys/test.py	13315
test_storage_iceberg_with_spark/test_local_table_safety.py	13305
test_keeper_four_word_command/test_allow_list.py	13247
test_storage_url_last_modified/test.py	13196
test_concurrent_queries_for_user_restriction/test.py	13181
test_geoparquet/test.py	13146
test_http_dictionary_named_collection/test.py	13057
test_parquet_page_index/test.py	13045
test_dictionaries_null_value/test.py	12995
test_keeper_path_acl/test.py	12814
test_prometheus_protocols/test_http_port.py	12794
test_keeper_secure_client/test.py	12781
test_storage_iceberg_with_spark/test_single_iceberg_file.py	12778
test_inherit_multiple_profiles/test.py	12706
test_system_grants_url_regexp/test.py	12703
test_storage_iceberg_with_spark/test_cluster_table_function_with_partition_pruning.py	12688
test_keeper_invalid_digest/test.py	12680
test_config_xml_main/test.py	12610
test_parallel_replicas_skip_shards/test.py	12575
test_system_users_predicate_pushdown/test.py	12528
test_hypothetical_projection_estimate_writes_nothing/test.py	12523
test_mutation_analyzer_setting/test.py	12490
test_structured_logging_json/test.py	12484
test_freeze_table/test.py	12481
test_reload_query_masking_rules/test.py	12442
test_config_hide_in_preprocessed/test.py	12399
test_keeper_memory_soft_limit_ratio/test.py	12386
test_buffer_profile/test.py	12338
test_disk_name_virtual_column/test.py	12318
test_allow_implicit_no_password/test.py	12280
test_backward_compatibility/test_insert_profile_events.py	12259
test_keeper_nuraft_streaming/test.py	12192
test_remap_executable/test.py	12178
test_s3_non_deterministic_partition_by/test.py	12152
test_keeper_http_storage_control/test.py	12107
test_compatibility_readonly_constrained_setting/test.py	12079
test_custom_dashboards/test.py	12052
test_config_decryption/test.py	12050
test_keeper_and_access_storage/test.py	12046
test_keeper_compression/test_without_compression.py	12046
test_keeper_compression/test_with_compression.py	12029
test_user_query_log_distributed_backend/test.py	12026
test_dictionaries_with_invalid_structure/test.py	12014
test_thread_pool_queue_size/test.py	11960
test_memory_thread_stacks_metric/test.py	11946
test_disk_types/test.py	11772
test_async_metrics_in_cgroup/test.py	11766
test_s3_redirect_remote_host_filter/test.py	11686
test_remote_function_view/test.py	11678
test_spark_session_recovery/test.py	11648
test_backup_s3_storage_class/test.py	11628
test_concurrent_hash_join_single_join_memory_limit/test.py	11626
test_arrowflight_interface/test_session_options_settings_profile.py	11574
test_remote_prewhere/test.py	11532
test_s3_storage_class/test.py	11517
test_passing_max_partitions_to_read_remotely/test.py	11512
test_server_initialization/test.py	11497
test_range_hashed_dictionary_types/test.py	11440
test_settings_randomization/test.py	11423
test_internal_queries_not_counted/test.py	11389
test_storage_iceberg_with_spark/test_writes_different_path_format_error.py	11322
test_play_image_preview/test.py	11210
test_core_dump_size_limit/test.py	11197
test_interserver_marker_requires_cluster_secret/test.py	11187
test_dotnet_client/test.py	11167
test_endpoint_macro_substitution/test.py	11167
test_server_keep_alive/test.py	11135
test_backward_compatibility/test_cte_distributed.py	11106
test_arrowflight_session_log/test.py	11051
test_profile_settings_and_constraints_order/test.py	11029
test_storage_url_with_proxy/test.py	10960
test_native_incorrect_data_deserialization/test.py	10938
test_overcommit_tracker/test.py	10932
test_cancel_freeze/test.py	10931
test_relative_filepath/test.py	10884
test_global_overcommit_tracker/test.py	10872
test_composable_protocol_without_global_ssl/test.py	10866
test_storage_iceberg_no_spark/test_iceberg_history_operation_summary.py	10808
test_send_crash_reports/test.py	10777
test_keeper_memory_soft_limit/test.py	10756
test_docs_web_ui_links/test.py	10746
test_play_chart_helpers/test.py	10715
test_arrowflight_interface/test_prepared_statement_limit.py	10667
test_storage_iceberg_no_spark/test_writes_parallel_replicas_no_catalog.py	10572
test_ssh/test_options_propagation_enabled.py	10558
test_backward_compatibility/test_old_client_with_replicated_columns.py	10540
test_play_result_shaping/test.py	10509
test_webterminal_startup/test.py	10435
test_prometheus_protocols/test_format_query_api.py	10419
test_memory_profiler_min_max_borders/test.py	10414
test_delayed_remote_source/test.py	10412
test_jemalloc_profiler_sampling_rate/test.py	10409
test_jemalloc_global_profiler/test.py	10378
test_merge_tree_check_part_with_cache/test.py	10376
test_filesystem_cache/test_size_limit_metric.py	10367
test_memory_limit/test.py	10358
test_format_cannot_allocate_thread/test.py	10344
test_async_metrics_overload_warning/test.py	10325
test_dirty_pages_force_purge/test.py	10299
test_trace_log_memory_context/test.py	10297
test_logs_level/test.py	10285
test_tcp_query_body_oversized_read/test.py	10283
test_tcp_handler_interserver_listen_host/test_case.py	10264
test_render_log_file_name_templates/test.py	10183
test_tcp_handler_http_responses/test_case.py	10085
test_host_regexp_hosts_file_resolution/test.py	10082
test_http_auth_config_credentials/test.py	10080
test_arrowflight_interface/test_prepared_statement_malformed_params.py	10060
test_cgroup_metrics/test.py	9989
test_storage_iceberg_no_spark/test_time_travel_bug_fix_validation.py	9961
test_filesystem_cache_uninitialized/test.py	9945
test_custom_http_handlers_per_protocol/test.py	9780
test_userspace_page_cache/test_incorrect_limits.py	9748
test_validate_threadpool_writer_pool_size/test.py	9708
test_system_reload_async_metrics/test_async_metrics_invalid_settings.py	9696
test_storage_iceberg_with_spark/test_compressed_metadata.py	9556
test_config_corresponding_root/test.py	9379
test_storage_iceberg_with_spark/test_relevant_iceberg_schema_chosen.py	9215
test_storage_iceberg_with_spark/test_minmax_pruning_for_arrays_and_maps_subfields_disabled.py	9203
test_keeper_watch_profile_events/test.py	9120
test_storage_iceberg_with_spark/test_dates.py	8768
test_storage_iceberg_no_spark/test_writes_v3_row_lineage.py	8700
test_database_disk/test.py	8520
test_keeper_http_control_readiness/test.py	8386
test_keeper_https_control_standalone_cluster/test.py	7476
test_storage_iceberg_no_spark/test_writes_create_table_bugs.py	7387
test_storage_iceberg_no_spark/test_graceful_error_not_configured_iceberg_metadata_log.py	7268
test_storage_iceberg_with_spark/test_variant_type.py	6304
test_keeper_http_control_cli/test.py	6285
test_keeper_request_total_with_subrequests/test.py	5981
test_concurrent_backups_s3/test.py	5938
test_keeper_http_jemalloc/test.py	5842
test_keeper_https_control_cli/test.py	5779
test_storage_iceberg_with_spark/test_multiple_partitions_on_one_column.py	5261
test_cgroup_limit/test.py	5183
test_kafka_bad_messages/test_delete_topic_helper.py	5006
test_kafka_bad_messages/test_admin_client_helper.py	4002
test_cluster_waiters/test_lost_network_interface.py	3558
test_disks_app_interactive/test.py	2507
test_jemalloc_percpu_arena/test.py	2403
test_clickhouse_test_abort_reap/test.py	1654
"""


def _parse_raw_durations(raw: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Accept both tab- and space-separated formats; last token is duration
        parts = line.split()
        try:
            duration = int(parts[-1])
        except Exception:
            continue
        path = " ".join(parts[:-1])
        out[path] = duration
    return out


TEST_DURATIONS: dict[str, int] = _parse_raw_durations(RAW_TEST_DURATIONS)


def get_tests_execution_time(info: Info, job_options: str) -> dict[str, int]:
    assert info.updated_at
    start_time_filter = f"parseDateTimeBestEffort('{info.updated_at}')"

    build = job_options.split(",", 1)[0]

    query = f"""
        SELECT
            file,
            round(sum(test_duration_ms)) AS file_duration_ms
        FROM
        (
            SELECT
                splitByString('::', test_name)[1] AS file,
                median(test_duration_ms) AS test_duration_ms
            FROM checks
            WHERE (check_name LIKE 'Integration tests%')
                AND (check_name LIKE '%{build}%')
                AND (check_start_time >= ({start_time_filter} - toIntervalDay(20)))
                AND (check_start_time <= ({start_time_filter} - toIntervalHour(5)))
                AND ((head_ref = 'master') AND startsWith(head_repo, 'ClickHouse/'))
                AND (file != '')
                AND (test_status != 'SKIPPED')
                AND (test_status != 'FAIL')
            GROUP BY test_name
        )
        GROUP BY file
        ORDER BY ALL
        SETTINGS use_query_cache = 1, query_cache_ttl = 432000, query_cache_nondeterministic_function_handling = 'save', query_cache_share_between_users = 1
        FORMAT JSON
    """

    client = CIDBCluster()
    print(query)
    try:
        res = client.do_select_query(query, retries=5, timeout=20)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        return {}

    if not res:
        return {}
    try:
        import json

        data = json.loads(res)
        return {row["file"]: int(row["file_duration_ms"]) for row in data["data"]}
    except Exception as e:
        print(f"ERROR: Failed to parse CIDB response: {e}")
        return {}


def get_optimal_test_batch(
    tests: list[str],
    total_batches: int,
    batch_num: int,
    num_workers: int,
    job_options: str,
    info: Info = None,
) -> tuple[list[str], list[str]]:
    """
    @tests - all tests to run
    @total_batches - total number of batches
    @batch_num - current batch number
    @num_workers - number of parallel workers in a batch
    returns optimal subset of parallel tests for batch_num and optimal subset of sequential tests for batch_num, based on data in TEST_DURATIONS.
    Test files not present in TEST_DURATIONS will be distributed by round robin.
    The function optimizes tail latency of batch with num_workers parallel workers.
    The function works in a deterministic way, so that batch calculated on the other machine with the same input generates the same result.
    """
    # parallel_skip_prefixes sanity check. On LLVM coverage jobs the caller has
    # already removed the tests matching LLVM_COVERAGE_SKIP_PREFIXES, so a
    # TEST_CONFIGS entry that falls entirely under a skip prefix is legitimately
    # absent there and must not trip the staleness check.
    _is_llvm_coverage = "amd_llvm_coverage" in (job_options or "")
    for test_config in TEST_CONFIGS:
        if _is_llvm_coverage and any(
            test_config.prefix.startswith(skip_prefix)
            for skip_prefix in LLVM_COVERAGE_SKIP_PREFIXES
        ):
            continue
        assert any(
            test_file.removeprefix("./").startswith(test_config.prefix)
            for test_file in tests
        ), f"No test files found for prefix [{test_config.prefix}] in [{tests}]"

    sequential_test_modules = [
        test_file
        for test_file in tests
        if any(
            test_file.startswith(test_config.prefix) and test_config.is_sequential
            for test_config in TEST_CONFIGS
        )
    ]
    parallel_test_modules = [
        test_file for test_file in tests if test_file not in sequential_test_modules
    ]

    if batch_num > total_batches:
        raise ValueError(f"batch_num must be in [1, {total_batches}], got {batch_num}")

    # Helper: group tests by their top-level directory (prefix)
    #  same prefix tests are grouped together to minimize docker pulls in test fixtures in each job batch
    def group_by_prefix(items: list[str]) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = {}
        for it in sorted(items):
            prefix = it.split("/", 1)[0]
            groups.setdefault(prefix, []).append(it)
        return groups

    # Parallel groups and Sequential groups separated to allow distinct packing
    parallel_groups = group_by_prefix(parallel_test_modules)
    sequential_groups = group_by_prefix(sequential_test_modules)

    durations = TEST_DURATIONS

    # Compute group durations as sum of known test durations within the group
    # TODO: fix in private
    #   ERROR: Failed to get secret [PRIVATE_CI_DB_URL]
    # Do NOT enable this: it makes job setup non-deterministic (distribution of tests among batches differ day-to-day),
    # breaks local reproducibility, and adds an external API dependency that reduces reliability.
    # if info and not info.is_local_run:
    #     durations = get_tests_execution_time(info, job_options)
    #     if not durations:
    #         print("WARNING: CIDB durations not found, using static TEST_DURATIONS")
    #         durations = TEST_DURATIONS

    def groups_with_durations(groups: dict[str, list[str]]):
        known_groups: list[tuple[str, int]] = []  # (prefix, duration)
        unknown_groups: list[str] = []  # prefixes with zero known duration
        for prefix, items in sorted(groups.items()):
            dur = sum(durations.get(t, 0) for t in items)
            if dur > 0:
                known_groups.append((prefix, dur))
            else:
                unknown_groups.append(prefix)
        # Sort known by (-duration, prefix) for deterministic LPT
        known_groups.sort(key=lambda x: (-x[1], x[0]))
        # Sort unknown prefixes to make RR deterministic
        unknown_groups.sort()
        return known_groups, unknown_groups

    p_known, p_unknown = groups_with_durations(parallel_groups)
    s_known, s_unknown = groups_with_durations(sequential_groups)

    # Sequential batches: start from scaled parallel weights to account for worker concurrency
    sequential_batches: list[list[str]] = [[] for _ in range(total_batches)]
    sequential_weights: list[int] = [0] * total_batches

    # LPT assign known-duration sequential groups
    for prefix, dur in s_known:
        idx = min(range(total_batches), key=lambda i: (sequential_weights[i], i))
        # prefix, dur sorted in s_known starting with longest duration - keep the order in batches to decrease tail latency
        sequential_batches[idx].extend(sequential_groups[prefix])
        sequential_weights[idx] += dur

    # Round-robin assign unknown-duration sequential groups
    for i, prefix in enumerate(s_unknown):
        idx = i % total_batches
        sequential_batches[idx].extend(sequential_groups[prefix])

    # Prepare batch containers and weights
    parallel_batches: list[list[str]] = [[] for _ in range(total_batches)]
    parallel_weights: list[int] = [w * num_workers for w in sequential_weights]

    # LPT assign known-duration parallel groups
    for prefix, dur in p_known:
        idx = min(range(total_batches), key=lambda i: (parallel_weights[i], i))
        # prefix, dur sorted in p_known starting with longest duration - keep the order in batches to decrease tail latency
        parallel_batches[idx].extend(parallel_groups[prefix])
        parallel_weights[idx] += dur

    # Sort tests within each batch by duration (longest first) to minimize tail latency
    # when tests are picked by workers from the queue
    for idx in range(total_batches):
        parallel_batches[idx].sort(key=lambda x: (-durations.get(x, 0), x))

    # Round-robin assign unknown-duration parallel groups
    for i, prefix in enumerate(p_unknown):
        idx = i % total_batches
        parallel_batches[idx].extend(parallel_groups[prefix])

    print(
        f"Batches parallel weights: [{[weight // num_workers // 1000 for weight in parallel_weights]}]"
    )

    # Sanity check (non-fatal): ensure total test count preserved
    total_assigned = sum(len(b) for b in parallel_batches) + sum(
        len(b) for b in sequential_batches
    )
    assert total_assigned == len(tests)

    return parallel_batches[batch_num - 1], sequential_batches[batch_num - 1]
