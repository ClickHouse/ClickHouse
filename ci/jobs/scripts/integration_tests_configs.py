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


# Test suite durations, used by get_optimal_test_batch to balance shards by duration.
# Regenerate periodically (it drifts as tests change) with the query below on play.clickhouse.com.
# The 20000ms floor keeps the table broad: suites without an entry get weight 0 and are only
# round-robin distributed, so the more wall-clock mass the table covers, the better the packer
# balances. At a 60000ms floor the table modelled only ~69% of the wall-clock mass, and the
# unmodelled remainder piled up unevenly enough to push a shard past the two-hour pytest
# session timeout; 20000ms brings coverage to ~96%. The path filter drops functional-test rows
# that share the integration check name. Drop rows whose test file no longer exists before
# pasting the result.
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
HAVING dur > 20000
ORDER BY dur DESC, test_suite ASC;
"""

RAW_TEST_DURATIONS = """
test_storage_s3_queue/test_6.py	1614610
test_storage_delta/test.py	1478248
test_storage_kafka/test_batch_fast.py	1412524
test_database_replicated_settings/test.py	1157639
test_replicated_database/test.py	1098390
test_storage_nats/test_nats_core.py	998084
test_max_bytes_ratio_before_external_order_group_by_for_server/test.py	989302
test_multiple_disks/test.py	964296
test_backup_restore_s3/test.py	956527
test_storage_rabbitmq/test.py	945030
test_keeper_session/test.py	934968
test_dictionaries_all_layouts_separate_sources/test_mongo.py	933274
test_backup_restore_new/test.py	891610
test_storage_s3/test.py	821952
test_refreshable_mat_view/test.py	820381
test_dictionaries_redis/test.py	812548
test_storage_s3_queue/test_system_stop.py	732918
test_postgresql_replica_database_engine/test_3.py	711804
test_storage_s3_queue/test_5.py	704816
test_distributed_load_balancing/test.py	703908
test_storage_azure_blob_storage/test.py	639896
test_storage_nats/test_nats_jet_stream.py	618208
test_restore_db_replica/test.py	605269
test_dictionaries_all_layouts_separate_sources/test_clickhouse_remote.py	588962
test_ttl_move/test.py	585793
test_dictionaries_all_layouts_separate_sources/test_clickhouse_local.py	584188
test_storage_kafka/test_system_stop.py	578670
test_dictionaries_all_layouts_separate_sources/test_https.py	576446
test_dictionaries_all_layouts_separate_sources/test_mysql.py	576446
test_dictionaries_all_layouts_separate_sources/test_http.py	573987
test_backup_restore_on_cluster/test_concurrency.py	568393
test_unknown_config_option/test.py	559200
test_storage_s3_queue/test_0.py	555512
test_async_load_databases/test.py	535401
test_mask_sensitive_info/test.py	508208
test_storage_s3_queue/test_2.py	505743
test_refreshable_mv/test.py	491080
test_distributed_ddl/test.py	490212
test_storage_iceberg_with_spark/test_minmax_pruning.py	480528
test_storage_nats/test_system_stop.py	469977
test_named_collections/test.py	455296
test_storage_kafka/test_partition_affinity.py	454758
test_backup_restore_on_cluster/test.py	451902
test_parallel_replicas_insert_select/test.py	450570
test_storage_s3_queue/test_3.py	442072
test_postgresql_replica_database_engine/test_1.py	425952
test_storage_rabbitmq/test_system_stop.py	422039
test_database_iceberg/test.py	416276
test_refreshable_mat_view_replicated/test.py	411613
test_storage_iceberg_with_spark/test_cluster_table_function.py	410386
test_concurrent_ttl_merges/test.py	402373
test_checking_s3_blobs_paranoid/test.py	390620
test_lost_part_during_startup/test.py	383986
test_dns_cache/test.py	379620
test_merge_tree_s3/test.py	375015
test_storage_s3_queue/test_1.py	374934
test_database_delta/test.py	373462
test_storage_iceberg_schema_evolution/test_evolved_schema_simple.py	362590
test_executable_table_function/test.py	362300
test_mysql_database_engine/test.py	360678
test_scheduler_io/test.py	352402
test_ttl_replicated/test.py	346567
test_storage_kafka/test_batch_slow_1.py	345339
test_kafka_bad_messages/test.py	343404
test_storage_hdfs/test.py	339666
test_filesystem_cache/test.py	338755
test_storage_kafka/test_batch_slow_4.py	335071
test_named_collections_encrypted2/test.py	332234
test_mysql57_database_engine/test.py	330102
test_mysql_protocol/test.py	319556
test_filesystem_split_cache/test.py	318195
test_database_glue/test.py	317398
test_drop_is_lock_free/test.py	317218
test_dictionaries_dependency/test.py	315852
test_distributed_directory_monitor_split_batch_on_failure/test.py	304212
test_dictionaries_ddl/test.py	303523
test_ytsaurus/test_tables.py	300808
test_crash_log/test.py	297740
test_postgresql_replica_database_engine/test_2.py	295014
test_row_policy/test.py	292207
test_storage_iceberg_with_spark/test_partition_pruning.py	288359
test_azure_403_handling/test.py	281969
test_prometheus_protocols/test_evaluation.py	277548
test_storage_kafka/test_batch_slow_5.py	273904
test_storage_iceberg_with_spark/test_expire_snapshots.py	273162
test_dictionaries_all_layouts_separate_sources/test_file.py	270266
test_storage_iceberg_with_spark/test_system_iceberg_metadata.py	268841
test_group_by_top_k_distributed/test.py	266738
test_storage_kafka/test_compression_codec.py	258217
test_parallel_replicas_invisible_parts/test.py	256369
test_postpone_failed_tasks/test.py	255432
test_statistics_cache/test.py	254040
test_backward_compatibility/test_aggregate_function_state.py	251258
test_insert_distributed_async_send/test.py	250273
test_s3_plain_rewritable/test.py	249430
test_create_handler/test.py	248578
test_ai_functions/test.py	245499
test_storage_bigquery/test.py	245449
test_storage_iceberg_schema_evolution/test_tuple_evolved_simple.py	244741
test_table_db_num_limit/test.py	241678
test_storage_kafka/test_batch_slow_6.py	240738
test_refreshable_mv_no_multi_read/test.py	240204
test_hedged_requests/test.py	239872
test_cleanup_dir_after_bad_zk_conn/test.py	236837
test_keeper_ttl_nodes/test.py	235552
test_backup_restore_on_cluster/test_cancel_backup.py	229633
test_parallel_replicas_over_distributed/test.py	228170
test_postgresql_replica_database_engine/test_0.py	227845
test_polymorphic_parts/test.py	226334
test_throttling/test.py	224267
test_storage_kafka/test_batch_slow_0.py	218460
test_storage_s3_queue/test_4.py	216592
test_http_handlers_config/test.py	212230
test_storage_iceberg_with_spark/test_position_deletes.py	207288
test_storage_postgresql/test.py	205676
test_implicit_index_upgrade/test.py	204640
test_storage_s3_queue/test_flush.py	201785
test_storage_iceberg_schema_evolution/test_array_evolved_nested.py	199045
test_dictionaries_all_layouts_separate_sources/test_executable_hashed.py	196378
test_storage_mysql/test.py	195024
test_broken_projections/test.py	193352
test_postgresql_database_engine/test.py	191516
test_dictionaries_all_layouts_separate_sources/test_mongo_uri.py	188548
test_scheduler_memory/test.py	185978
test_storage_mongodb/test.py	185322
test_backup_restore_new/test_cancel_backup.py	184917
test_keeper_container_nodes/test.py	184373
test_storage_iceberg_no_spark/test_writes_statistics_by_minmax_pruning.py	181830
test_backward_compatibility/test_convert_ordinary.py	180804
test_lost_part/test.py	179754
test_grant_and_revoke/test_with_table_engine_grant.py	179490
test_replicated_users/test.py	177564
test_replicated_mutations/test.py	176628
test_jbod_balancer/test.py	176283
test_hive_query/test.py	175558
test_disk_over_web_server/test.py	175147
test_storage_kerberized_kafka/test.py	174318
test_ytsaurus/test_dictionaries.py	174186
test_merge_tree_azure_blob_storage/test.py	173120
test_executable_udf_async_metrics/test.py	172443
test_quorum_inserts/test.py	168892
test_system_logs/test_system_logs.py	167314
test_storage_kafka/test_batch_slow_2.py	167185
test_storage_kafka/test_kafka_zone_awareness.py	166565
test_distributed_frozen_replica/test.py	165701
test_replicated_fetches_bandwidth/test.py	165308
test_rename_column/test.py	165300
test_cluster_discovery/test.py	163583
test_keeper_two_nodes_cluster/test.py	162850
test_storage_iceberg_with_spark/test_writes.py	162632
test_stop_insert_when_disk_close_to_full/test.py	162492
test_plain_rewritable_backward_compatibility/test.py	161057
test_text_index_upgrade/test.py	160393
test_dictionaries_all_layouts_separate_sources/test_executable_cache.py	159683
test_create_as_select_on_cluster_distributed/test.py	159201
test_parallel_replicas_custom_key_failover/test.py	159000
test_keeper_snapshot_chunked_transfer/test.py	156236
test_manipulate_statistics/test.py	155230
test_keeper_internal_secure/test.py	154314
test_drop_database_replica/test.py	153374
test_dictionaries_update_and_reload/test.py	153048
test_storage_iceberg_with_spark/test_remove_orphan_files.py	153008
test_s3_plain_rewritable_rotate_tables/test.py	151804
test_s3_aws_sdk_has_slightly_unreliable_behaviour/test.py	149976
test_encrypted_disk/test.py	149657
test_prometheus_protocols/test_different_table_engines.py	146127
test_mutations_with_tampered_parts/test.py	144742
test_storage_iceberg_with_spark/test_writes_mutate_delete.py	144600
test_database_backup/test.py	144472
test_keeper_zookeeper_converter/test.py	143572
test_restore_replica/test.py	143471
test_keeper_map/test.py	140644
test_transactions/test.py	139218
test_storage_url/test.py	139034
test_system_clusters_actual_information/test.py	137812
test_log_query_probability/test.py	137780
test_default_session_user/test.py	136506
test_storage_iceberg_disks/test.py	135113
test_refreshable_mv_skip_old_temp_table_ddls/test.py	134888
test_scheduler_query/test.py	132226
test_kafka_bad_messages/test_mv_target_missing.py	131738
test_system_logs_recreate/test.py	129763
test_distributed_index_analysis/test.py	129226
test_parallel_replicas_custom_key_load_balancing/test.py	128041
test_attach_without_fetching/test.py	127827
test_ddl_worker_replicas/test.py	127399
test_storage_kafka/test_keeper_session_loss_direct_read.py	127341
test_quota/test.py	126338
test_merges_memory_limit/test.py	122328
test_corrupted_part_files/test.py	121937
test_distributed_ddl_parallel/test.py	121760
test_recompression_ttl/test.py	121430
test_postgresql_ssl/test.py	121000
test_disk_access_storage/test.py	119726
test_recovery_replica/test.py	118807
test_version_update_after_mutation/test.py	118640
test_MemoryTracking/test.py	118368
test_merge_tree_hdfs/test.py	118036
test_storage_iceberg_schema_evolution/test_tuple_evolved_nested.py	117704
test_storage_iceberg_with_spark/test_writes_create_partitioned_table.py	116120
test_disk_configuration/test.py	115501
test_storage_kafka/test_produce_http_interface.py	115121
test_named_collections_encrypted2/test_integr.py	113244
test_mutations_with_merge_tree/test.py	112688
test_cluster_discovery/test_auxiliary_keeper.py	111354
test_reloading_storage_configuration/test.py	110516
test_replicated_user_defined_functions/test.py	110220
test_storage_hudi/test.py	109424
test_storage_iceberg_with_spark/test_manifest_compaction.py	108748
test_zookeeper_config/test_secure.py	108572
test_partition/test.py	108191
test_system_merges/test.py	107642
test_database_remote/test.py	107589
test_distributed_inter_server_secret/test.py	107500
test_backup_restore_on_cluster_with_checksum_data_file_name/test.py	107096
test_migration_deduplication_hash/test.py	106524
test_database_disk_setting/test.py	104954
test_alter_settings_or_comment_on_cluster/test.py	104316
test_paimon_incremental_read/test.py	103450
test_backup_restore_azure_blob_storage/test.py	103334
test_dictionaries_postgresql/test.py	102870
test_executable_dictionary/test.py	102442
test_alter_moving_garbage/test.py	101908
test_allowed_url_from_config/test.py	101047
test_s3_table_functions/test.py	100862
test_merge_tree_load_parts/test.py	100523
test_backup_restore_keeper_map/test.py	99192
test_dictionary_lazy_load/test.py	98746
test_s3_cluster/test.py	98545
test_scheduler_cpu/test.py	98167
test_storage_delta_disks/test.py	97796
test_drop_replica_with_auxiliary_zookeepers/test.py	96754
test_dictionaries_mysql/test.py	96247
test_executable_user_defined_function/test.py	96201
test_arrowflight_interface/test_sql_server.py	96074
test_distributed_plan_replicated_merge_tree/test.py	95957
test_modify_engine_on_restart/test_ordinary.py	95658
test_settings_profile/test.py	94710
test_globs_in_filepath/test.py	93718
test_keeper_reconfig_replace_leader_in_one_command/test.py	92443
test_keeper_back_to_back/test.py	91988
test_storage_iceberg_with_spark/test_query_condition_cache.py	91936
test_azure_blob_storage_plain_rewritable/test.py	91930
test_cluster_discovery/test_dynamic_clusters.py	91694
test_backward_compatibility/test_pr_protocol_with_stream_id.py	90636
test_executable_udf_profile_events/test.py	90588
test_system_start_stop_listen/test.py	90080
test_rocksdb_options/test.py	89950
test_check_table/test.py	88971
test_distributed_ddl/test_replicated_alter.py	88410
test_s3_cluster_restart/test.py	88406
test_random_inserts/test.py	88404
test_hedged_requests_parallel/test.py	88302
test_keeper_incorrect_config/test.py	88139
test_restore_replica_metadata_version/test.py	88064
test_storage_iceberg_with_spark/test_metadata_file_format_with_uuid.py	87770
test_keeper_remove_rejoin_leader/test.py	87460
test_keeper_password/test.py	87344
test_storage_iceberg_with_spark/test_metadata_file_selection.py	86664
test_storage_iceberg_with_spark/test_writes_mutate_update.py	86466
test_format_schema_source/test.py	86278
test_keeper_three_nodes_two_alive/test.py	85959
test_scheduler_cpu_preemptive/test.py	85598
test_storage_iceberg_with_spark/test_schema_evolution_with_time_travel.py	85204
test_backup_restore_on_cluster/test_disallow_concurrency.py	84994
test_server_reload/test.py	84969
test_backup_source_grants/test.py	84840
test_storage_s3_queue/test_parallel_inserts.py	84574
test_modify_engine_on_restart/test.py	84511
test_access_control_with_custom_setup/test.py	83938
test_restore_external_engines/test.py	83824
test_remote_blobs_naming/test_backward_compatibility.py	83542
test_system_metrics/test.py	83253
test_s3_table_function_with_http_proxy/test.py	83094
test_truncate_database/test_distributed.py	82908
test_ddl_create_then_alter_offline_replica/test.py	82906
test_storage_iceberg_with_spark/test_schema_inference.py	82762
test_s3_table_function_with_https_proxy/test.py	82639
test_zookeeper_send_window_broken_promise/test.py	82546
test_jbod_ha/test.py	82194
test_keeper_opentelemetry_tracing/test.py	81764
test_refreshable_mv_watch_fault/test.py	81616
test_system_flush_logs/test.py	80884
test_store_cleanup/test.py	80378
test_rmv_access_denied_on_rename_race/test.py	80275
test_always_fetch_merged/test.py	79852
test_replicated_merge_tree_compatibility/test.py	79722
test_clickhouse_server_wait_server_pool/test.py	79470
test_replicated_database_interserver_host/test.py	79464
test_replication_credentials/test.py	79376
test_parallel_replicas_custom_key/test.py	78910
test_https_replication/test.py	78908
test_consistant_parts_after_move_partition/test.py	78826
test_query_runner/test.py	78471
test_dictionaries_dependency_xml/test.py	77954
test_s3_credentials_hardening/test.py	77132
test_client_auto_secure_port/test.py	76895
test_catboost_evaluate/test.py	76775
test_mark_cache_profile_events/test.py	75542
test_file_schema_inference_cache/test.py	75313
test_storage_kafka_sasl/test.py	74574
test_keeper_force_recovery/test.py	74249
test_keeper_ttl_nodes/test_disabled.py	74023
test_storage_kafka/test_avro_schema_registry.py	73893
test_database_catalog_shutdown_system_logs/test.py	73881
test_keeper_disks/test.py	73542
test_replicated_merge_tree_encryption_codec/test.py	73432
test_index_filename_upgrade/test.py	73087
test_log_family_hdfs/test.py	73019
test_user_memory_tracker_log_drift/test.py	72850
test_storage_iceberg_with_spark/test_writes_create_table.py	72596
test_insert_into_distributed/test.py	72490
test_storage_iceberg_schema_evolution/test_array_evolved_with_struct.py	72046
test_multi_access_storage_role_management/test.py	71859
test_insert_distributed_load_balancing/test.py	71602
test_replicated_table_attach/test.py	71274
test_inserts_with_keeper_retries/test.py	71158
test_parallel_replicas_distributed_skip_shards/test.py	70975
test_keeper_container_nodes/test_disabled.py	70974
test_keeper_persistent_watches/test.py	70008
test_storage_s3_queue/test_sts_smoke.py	70002
test_server_overload/test.py	69890
test_replicated_database_cluster_groups/test.py	69814
test_sparsity_exact_num_defaults_compat/test.py	69577
test_merge_tree_s3_failover/test.py	69423
test_storage_iceberg_schema_evolution/test_evolved_schema_complex.py	69020
test_replicated_database_recover_digest_mismatch/test.py	69011
test_default_compression_codec/test.py	68530
test_keeper_nodes_remove/test.py	67988
test_server_metadata_files/test.py	67297
test_sharding_key_from_default_column/test.py	67040
test_backward_compatibility/test_aggregate_function_state_contingency_functions.py	66897
test_replicated_merge_tree_wait_on_shutdown/test.py	66611
test_storage_iceberg_with_trino/test.py	66432
test_redirect_url_storage/test.py	66329
test_string_aggregation_compatibility/test.py	65654
test_named_collections_if_exists_on_cluster/test.py	65646
test_create_union_system_log_tables/test.py	65420
test_storage_iceberg_with_spark/test_iceberg_snapshot_reads.py	65013
test_storage_kafka/test_poll_timeout_after_assignment.py	64338
test_postgresql_remote_host_filter/test.py	64219
test_parallel_replicas_snapshot_from_initiator/test.py	64069
test_keeper_4lw_reconfiguration/test.py	63756
test_async_insert_memory/test.py	63384
test_attach_tampered_detached_parts/test.py	63186
test_alter_on_mixed_type_cluster/test.py	62562
test_graphite_merge_tree/test.py	62487
test_http_failover/test.py	62470
test_packed_io/test.py	62313
test_graphite_merge_tree_typed/test.py	61510
test_backup_restore_on_cluster/test_huge_concurrent_restore.py	61464
test_storage_iceberg_with_spark/test_explicit_metadata_file.py	61055
test_backward_compatibility/test_block_marshalling.py	60592
test_lightweight_updates/test.py	60458
test_host_regexp_multiple_ptr_records/test.py	60158
test_auth_method_grants_deferred_expiry/test.py	59956
test_background_operations_config/test.py	59689
test_keeper_max_append_byte_size/test.py	59606
test_limited_replicated_fetches/test.py	59526
test_group_array_element_size/test.py	59478
test_dictionaries_replace/test.py	59475
test_distributed_format/test.py	59300
test_azure_blob_storage_native_copy/test.py	59190
test_settings_constraints/test.py	59131
test_keeper_auth/test.py	58962
test_phantom_parts_in_mutations/test.py	58853
test_backup_restore_new/test_shutdown_wait_backup.py	58331
test_table_function_mongodb/test.py	58189
test_executable_pool_udf_profile_events/test.py	58094
test_storage_iceberg_with_spark/test_writes_schema_evolution.py	58018
test_delayed_replica_failover/test.py	58011
test_allow_feature_tier/test.py	57908
test_storage_iceberg_with_spark/test_delete_files.py	57793
test_keeper_reconfig_remove_many/test.py	57761
test_storage_redis/test.py	57304
test_keeper_snapshot_small_distance/test.py	57204
test_zookeeper_config_load_balancing/test.py	57192
test_no_merges_volume_ttl/test.py	57191
test_distributed_ddl_password/test.py	57125
test_send_request_to_leader_replica/test.py	56988
test_grant_and_revoke/test_without_table_engine_grant.py	56898
test_replicated_merge_tree_encrypted_disk/test.py	56840
test_backward_compatibility/test_aggregate_function_state_tuple_return_type.py	56764
test_backup_restore_on_cluster/test_two_shards_two_replicas.py	56716
test_storage_iceberg_concurrent/test_concurrent_reads.py	56600
test_system_detached_tables/test.py	56505
test_backups_from_disk/test.py	55588
test_https_replication/test_change_ip.py	55496
test_storage_s3/test_sts.py	55378
test_lightweight_updates_compatibility/test.py	55206
test_attach_partition_using_copy/test.py	54963
test_ddl_on_cluster_stop_waiting_for_offline_hosts/test.py	54573
test_async_insert_adaptive_busy_timeout/test.py	54453
test_named_collections_encrypted/test.py	54038
test_mysql_kill_query/test.py	53890
test_keeper_nodes_add/test.py	53885
test_system_ddl_worker_queue/test.py	53604
test_old_parts_finally_removed/test.py	53559
test_keeper_block_acl/test.py	53494
test_storage_iceberg_with_spark/test_async_metadata_refresh.py	53102
test_storage_iceberg_with_spark/test_format_version_upgrade.py	53038
test_keeper_mntr_pressure/test.py	53034
test_user_valid_until/test.py	52909
test_warning_broken_tables/test.py	52832
test_read_only_table/test.py	52681
test_storage_kafka/test_intent_sizes.py	52528
test_prometheus_protocols/test_compliance.py	52378
test_database_iceberg_lakekeeper_catalog/test.py	52367
test_storage_iceberg_schema_evolution/test_array_map_evolved_with_struct.py	52082
test_quorum_inserts_parallel/test.py	51992
test_memory_limit_observer/test.py	51770
test_storage_iceberg_with_spark/test_writes_with_partitioned_table.py	51612
test_disabled_access_control_improvements/test_row_policy.py	51370
test_storage_numbers/test.py	51361
test_replicated_merge_tree_with_auxiliary_zookeepers/test.py	51334
test_backward_compatibility/test_bucketed_map_order.py	51160
test_nullable_tuple_subcolumns/test.py	50927
test_replace_partition/test.py	50768
test_storage_iceberg_with_spark/test_read_in_order.py	50660
test_mutations_in_partitions_of_merge_tree/test.py	50232
test_reload_auxiliary_zookeepers/test.py	49839
test_version_update/test.py	49812
test_storage_iceberg_with_spark/test_bucket_partition_pruning.py	49800
test_max_suspicious_broken_parts_replicated/test.py	49706
test_storage_alias_replicated/test.py	49595
test_user_directories/test.py	49553
test_temporary_data_in_cache/test.py	49481
test_consistent_parts_after_clone_replica/test.py	49456
test_replicated_access/test.py	49371
test_tmp_policy/test.py	49331
test_modify_engine_on_restart/test_unsafe_name.py	49300
test_keeper_map_retries/test.py	49257
test_concurrent_queries_restriction_by_query_kind/test.py	49064
test_backward_compatibility/test_parallel_replicas_protocol.py	48398
test_fetch_partition_should_reset_mutation/test.py	48374
test_database_iceberg_nessie_catalog/test.py	48324
test_storage_kafka/test_schema_registry_skip_bytes.py	48316
test_encrypted_disk_replication/test.py	48096
test_keeper_readahead/test.py	47912
test_config_substitutions/test.py	47727
test_backup_restore_on_cluster/test_slow_rmt.py	47681
test_fetch_partition_from_auxiliary_zookeeper/test.py	47646
test_storage_kafka/test_zookeeper_locks.py	47641
test_matview_union_replicated/test.py	47606
test_kafka_bad_messages/test_1.py	47457
test_statistics_minmax_upgrade/test.py	47285
test_grpc_protocol/test.py	47163
test_backward_compatibility/test_aggregation_with_out_of_order_buckets.py	46892
test_https_s3_table_function_with_http_proxy_no_tunneling/test.py	46838
test_distributed_insert_backward_compatibility/test.py	46464
test_on_cluster_timeouts/test.py	46233
test_reload_clusters_config/test.py	46054
test_fetch_partition_with_outdated_parts/test.py	46009
test_keeper_as_server/test.py	45926
test_backup_restore_on_cluster_s3_credentials/test.py	45840
test_backup_restore/test.py	45800
test_zookeeper_config/test.py	45634
test_non_default_compression/test.py	45552
test_keeper_znode_time/test.py	45532
test_race_condition_for_replicated_merge_tree/test.py	45419
test_settings_constraints_distributed/test.py	45338
test_distributed_config/test.py	45264
test_join_set_family_s3/test.py	44992
test_backup_restore_on_cluster/test_different_versions.py	44964
test_drop_replica/test.py	44854
test_replicated_merge_tree_s3/test.py	44717
test_replicated_s3_zero_copy_drop_partition/test.py	44699
test_parallel_replicas_protocol/test.py	44576
test_storage_log_damaged_array/test.py	44565
test_disks_app_func/test.py	44364
test_postgresql_kill_query/test.py	43954
test_part_uuid/test.py	43806
test_variant_escaping_merge_tree_compatibility/test.py	43756
test_undrop_query/test.py	43746
test_cross_replication/test.py	43406
test_concurrent_threads_soft_limit/test.py	42981
test_http_connection_socket_buffer_settings/test.py	42938
test_zookeeper_connection_log/test.py	42589
test_ldap_external_user_directory/test.py	42545
test_keeper_snapshot_chunked_transfer/test_concurrent.py	41977
test_storage_s3/test_invalid_env_credentials.py	41971
test_keeper_multinode_simple/test.py	41895
test_keeper_four_word_command/test.py	41459
test_replicated_fetches_min_part_level/test.py	41372
test_storage_kafka/test_batch_slow_7.py	41340
test_file_cluster/test.py	41312
test_dictionaries_redis/test_long.py	40812
test_attach_with_different_projections_or_indices/test.py	40800
test_dictionary_ddl_on_cluster/test.py	40703
test_distributed_ddl_on_cross_replication/test.py	40694
test_ssl_cert_authentication/test.py	40587
test_dictionaries_config_reload/test.py	40260
test_force_drop_table/test.py	40252
test_insert_over_http_query_log/test.py	39984
test_keeper_feature_flags_config/test.py	39973
test_s3_cluster_insert_select/test.py	39883
test_arrowflight_interface/test.py	39828
test_disabled_access_control_improvements/test_users_without_row_policies_can_read_rows.py	39762
test_part_loading_tree_rollback/test.py	39680
test_keeper_nodes_move/test.py	39595
test_zookeeper_config/test_password.py	39555
test_arrowflight_storage/test.py	39507
test_storage_iceberg_with_spark_cache/test_metadata_cache.py	39480
test_keeper_session_refuse_stale_server/test.py	39476
test_backup_restore_storage_policy/test.py	39118
test_executable_user_defined_functions_config_reload/test.py	39100
test_modify_engine_on_restart/test_storage_policies.py	38645
test_alter_database_on_cluster/test.py	38555
test_ddl_worker_stale_task_name/test.py	38474
test_db_ordinary_deprecated_warning/test.py	38244
test_parts_delete_zookeeper/test.py	38237
test_backward_compatibility/test_ip_types_binary_compatibility.py	38198
test_config_decryption/test_wrong_settings.py	37894
test_storage_iceberg_with_spark/test_metadata_file_selection_from_version_hint.py	37822
test_keeper_broken_logs/test.py	37795
test_keeper_force_recovery_single_node/test.py	37702
test_storage_delta_shuffles/test.py	37614
test_storage_iceberg_interoperability_azure/test_interoperability.py	37522
test_parallel_replicas_failover/test.py	37495
test_keeper_snapshots/test.py	37374
test_alternative_keeper_config/test.py	37298
test_backward_compatibility/test_cte_distributed.py	37239
test_storage_iceberg_schema_evolution/test_map_evolved_nested.py	37106
test_force_deduplication/test.py	36982
test_transposed_metric_log/test.py	36876
test_zero_copy_drop_table_with_leftover/test.py	36876
test_database_hms/test.py	36579
test_database_hms/test_ttransport_exception_reproduction.py	36555
test_odbc_interaction/test.py	36470
test_storage_delta/test_cdf.py	36308
test_keeper_max_request_size/test.py	36070
test_atomic_drop_table/test.py	36056
test_keeper_log_gap_before_committed/test.py	35983
test_move_partition_to_volume_async/test.py	35948
test_storage_iceberg_with_spark/test_optimize.py	35930
test_prometheus_protocols/test_upgrade_from_prealpha.py	35896
test_dictionaries_select_all/test.py	35819
test_s3_storage_conf_proxy/test.py	35805
test_reload_zookeeper/test.py	35798
test_reload_client_certificate/test.py	35788
test_storage_iceberg_no_spark/test_writes_rename_column.py	35569
test_ddl_alter_query/test.py	35515
test_storage_iceberg_with_spark/test_file_stats_logging.py	35498
test_storage_azure_blob_storage/test_cluster.py	35460
test_replicated_fetches_timeouts/test.py	35442
test_keeper_reconfig_replace_leader/test.py	35440
test_keeper_reconfig_remove/test.py	35426
test_parallel_replicas_alias_columns/test.py	35266
test_dremio_engine/test.py	35004
test_max_suspicious_broken_parts/test.py	34881
test_profile_max_sessions_for_user/test.py	34847
test_prometheus_endpoint/test.py	34770
test_backward_compatibility/test_adaptive_codec.py	34762
test_storage_iceberg_with_spark/test_explanation.py	34722
test_attach_partition_with_large_destination/test.py	34660
test_startup_scripts/test.py	34598
test_mutations_with_projection/test.py	34581
test_keeper_dynamic_settings/test.py	34475
test_remove_stale_moving_parts/test.py	34462
test_postgresql_protocol/test.py	34407
test_distributed_async_insert_for_node_changes/test.py	34205
test_storage_iceberg_interoperability_local/test_interoperability.py	34136
test_keeper_s3_snapshot/test.py	33975
test_select_access_rights/test_from_system_tables.py	33969
test_access_control_on_cluster/test.py	33940
test_modify_engine_on_restart/test_mv.py	33554
test_session_log/test.py	33520
test_keeper_snapshot_on_exit/test.py	33491
test_disk_checker/test.py	33076
test_restart_server/test.py	33044
test_storage_s3_queue/test_dimensional_metrics.py	33014
test_distributed_respect_user_timeouts/test.py	32886
test_replication_without_zookeeper/test.py	32838
test_secure_socket/test.py	32836
test_zookeeper_fallback_session/test.py	32574
test_force_restore_data_flag_for_keeper_dataloss/test.py	32413
test_keeper_snapshot_rotation_race/test.py	32282
test_acme_tls/test_multi_node.py	31872
test_hot_reload_storage_policy/test.py	31626
test_user_query_log_config_validation/test.py	31602
test_ddl_worker_non_leader/test.py	31594
test_rocksdb_read_only/test.py	31531
test_server_startup_and_shutdown_logs/test.py	31436
test_suggestions/test.py	31419
test_mongodb_kill_query/test.py	31265
test_modify_engine_on_restart/test_zk_path_exists.py	31180
test_asynchronous_metrics_pk_bytes_fields/test.py	31176
test_distributed_structure_fetch/test.py	31156
test_reloading_settings_from_users_xml/test.py	31131
test_analyzer_compatibility/test.py	31078
test_ttl_multilevel_group_by/test.py	30614
test_webassembly_udf/test.py	30594
test_materialize_projections_on_merge/test.py	30570
test_search_orphaned_parts/test.py	30520
test_compression_nested_columns/test.py	30235
test_access_cache_recompute_coalescing/test.py	30219
test_dictionaries_wait_for_load/test.py	30203
test_keeper_persistent_log_multinode/test.py	30055
test_distributed_plan_worker_exchange_port/test.py	30042
test_compressed_marks_restart/test.py	30040
test_backward_compatibility/test_functions.py	29892
test_acme_tls/test_single_node.py	29890
test_cache_bypass_on_disk_failure/test.py	29784
test_keeper_reconfig_add/test.py	29784
test_disabled_access_control_improvements/test_select_from_system_tables.py	29727
test_concurrent_part_removal_threshold_for_remote_disk/test.py	29720
test_storage_s3_queue/test_file_iterator_ttl.py	29648
test_merge_tree_s3_with_cache/test.py	29612
test_play_reconcile_startup/test.py	29565
test_disable_insertion_and_mutation/test.py	29228
test_modify_engine_on_restart/test_args.py	29224
test_parallel_replicas_no_replicas/test.py	29186
test_parallel_replicas_all_marks_read/test.py	29138
test_sync_replica_on_cluster/test.py	29135
test_user_defined_object_persistence/test.py	29116
test_storage_iceberg_with_spark/test_column_names_with_dots.py	29094
test_format_avro_confluent/test.py	29030
test_storage_iceberg_with_spark/test_writes_field_ids_spark_read.py	28974
test_modify_engine_on_restart/test_unusual_path.py	28924
test_backup_restore_s3/test_throttling.py	28916
test_shutdown_wait_unfinished_queries/test.py	28864
test_rabbitmq_malicious_broker/test.py	28860
test_keeper_azure_s3_plain/test.py	28843
test_sqlite_kill_query/test.py	28826
test_auth_method_grants_on_cluster/test.py	28784
test_auth_method_valid_until_stateful_protocols/test.py	28694
test_ddl_worker_with_loopback_hosts/test.py	28643
test_log_family_s3/test.py	28631
test_keeper_unpreprocessed_logs_livelock/test.py	28543
test_system_queries/test.py	28397
test_permissions_drop_replica/test.py	28375
test_parallel_replicas_skip_inactive_replicas/test.py	28365
test_dictionary_asynchronous_metrics/test.py	28360
test_parallel_replicas_skip_inactive_replicas_all_groups/test.py	28357
test_validate_only_initial_alter_query/test_replicated_database.py	28314
test_match_process_uid_against_data_owner/test.py	28256
test_drop_if_empty/test.py	28212
test_dictionary_allow_read_expired_keys/test_dict_get_or_default.py	28081
test_keeper_follower_metrics/test.py	28069
test_paimon_rest_catalog/test.py	27955
test_placement_info/test.py	27861
test_insert_into_distributed_sync_async/test.py	27820
test_filesystem_cache_eviction_metrics/test.py	27730
test_keeper_raft_cert_reload/test.py	27634
test_dictionary_allow_read_expired_keys/test_dict_get.py	27618
test_experimental_codec_config_default/test.py	27533
test_covered_by_broken_exists/test.py	27530
test_compatibility_merge_tree_settings/test.py	27524
test_filesystem_layout/test.py	27454
test_dictionary_allow_read_expired_keys/test_default_reading.py	27377
test_storage_iceberg_with_spark/test_minmax_pruning_with_null.py	27304
test_system_logs_hostname/test_replicated.py	27284
test_insert_into_distributed_through_materialized_view/test.py	27282
test_keeper_persistent_log/test.py	27268
test_reset_ddl_worker/test.py	27250
test_storage_iceberg_with_spark/test_writes_from_zero.py	27150
test_fix_metadata_version/test.py	26990
test_ssh/test.py	26911
test_extreme_deduplication/test.py	26769
test_keeper_remove_acl/test.py	26748
test_part_log_table/test.py	26561
test_replica_is_active/test.py	26552
test_bind_host/test.py	26454
test_s3_access_headers/test.py	26435
test_replicated_access/test_invalid_entity.py	26373
test_cleanup_after_start/test.py	26303
test_executable_udf_names_in_system_query_log/test.py	26286
test_backup_log/test.py	26270
test_max_rows_to_read_leaf_with_view/test.py	26268
test_ddl_worker_retry_when_dropping_db_failed/test.py	26087
test_check_table_name_length_2/test.py	26040
test_settings_constraints_distributed_ddl/test.py	25906
test_mutation_fetch_fallback/test.py	25857
test_storage_iceberg_no_spark/test_iceberg_history_large_summary.py	25825
test_detached_parts_metrics/test.py	25784
test_settings_from_server/test.py	25677
test_alter_comment_on_cluster/test.py	25480
test_disabled_mysql_server/test.py	25464
test_server_start_and_ip_conversions/test.py	25379
test_topk_alpha_map_compatibility/test.py	25370
test_parallel_replicas_increase_error_count/test.py	25269
test_keeper_three_nodes_start/test.py	25052
test_storage_iceberg_schema_evolution/test_correct_column_mapper_is_chosen.py	24826
test_no_password_existing_user/test.py	24654
test_userspace_page_cache/test.py	24632
test_access_for_functions/test.py	24614
test_parallel_replicas_insert_select_coordinator_reuse/test.py	24586
test_default_database_on_cluster/test.py	24537
test_distributed_ddl_on_database_cluster/test.py	24525
test_table_function_redis/test.py	24476
test_azure_blob_storage_listobjects_prefix/test.py	24305
test_default_compression_in_mergetree_settings/test.py	24294
test_old_versions/test.py	24274
test_keeper_leader_metrics/test.py	24272
test_alter_settings_on_cluster/test.py	24250
test_cow_policy/test.py	24182
test_acme_tls/test_no_certificate.py	24146
test_optimize_on_insert/test.py	24045
test_broken_part_during_merge/test.py	23992
test_external_cluster/test.py	23990
test_threadpool_readers/test.py	23886
test_log_lz4_streaming/test.py	23874
test_zero_copy_expand_macros/test.py	23834
test_mutations_hardlinks/test.py	23762
test_zero_copy_lock_leak/test.py	23708
test_interserver_dns_retires/test.py	23666
test_keeper_snapshots_multinode/test.py	23658
test_asynchronous_metric_log_table/test.py	23645
test_early_memory_limit_exception/test.py	23487
test_storage_iceberg_with_spark/test_writes_complex_type.py	23454
test_broken_tmp_txn_version_startup/test.py	23398
test_truncate_database/test_replicated.py	23396
test_intersecting_parts/test.py	23390
test_ddl_config_hostname/test.py	23381
test_keeper_mntr_data_size/test.py	23284
test_totp_auth/test_totp.py	23186
test_keeper_restore_from_snapshot/test_disk_s3.py	23177
test_storage_iceberg_no_spark/test_cluster_partition_pruning_reads.py	23144
test_move_partition_to_disk_on_cluster/test.py	23115
test_storage_s3/test_parquet_prewhere.py	23014
test_profile_events_s3/test.py	22955
test_groupBitmapAnd_on_distributed/test.py	22918
test_max_authentication_methods_per_user/test.py	22902
test_recovery_time_metric/test.py	22853
test_cluster_discovery/test_password.py	22834
test_parameterized_view/test.py	22797
test_peak_memory_usage/test.py	22785
test_external_http_authenticator/test.py	22770
test_paimon_metadata_files_cache/test.py	22745
test_replicated_database_alter_modify_order_by/test.py	22740
test_azure_disk_unreachable/test.py	22724
test_deduplicated_attached_part_rename/test.py	22690
test_sql_user_defined_functions_on_cluster/test.py	22496
test_storage_iceberg_schema_evolution/test_full_drop.py	22393
test_replicated_merge_tree_thread_schedule_timeouts/test.py	22388
test_groupBitmapAnd_on_distributed/test_groupBitmapAndState_on_distributed_table.py	22383
test_replicated_database_with_auxiliary_zookeepers/test.py	22356
test_s3_low_cardinality_right_border/test.py	22253
test_format_schema_on_server/test.py	22246
test_aliases_in_default_expr_not_break_table_structure/test.py	22240
test_s3_storage_conf_new_proxy/test.py	22178
test_parallel_replicas_local_replica_forced_inactive/test.py	22113
test_ldap_follow_referrals/test.py	22090
test_auth_method_grants_disabled_method/test.py	22070
test_limit_by_transform_kill_query/test.py	22042
test_prefer_global_in_and_join/test.py	21741
test_prometheus_protocols/test_write_read.py	21740
test_point_in_polygon_cache_size/test.py	21660
test_storage_delta/test_imds.py	21652
test_executable_user_defined_function/test_system_table.py	21610
test_storage_iceberg_no_spark/test_local_path_traversal.py	21588
test_executable_user_defined_function_lifetime_reload/test.py	21566
test_replicated_database_system_clusters_log_level/test.py	21496
test_backward_compatibility/test_nullable_sparse_compatibility.py	21494
test_storage_iceberg_no_spark/test_read_in_order_with_pyiceberg.py	21484
test_merge_tree_empty_parts/test.py	21458
test_prometheus_before_tables/test.py	21458
test_storage_iceberg_with_spark_cache/test_filesystem_cache.py	21451
test_http_limits/test_hard_limit.py	21375
test_sql_roles_for_xml_users/test.py	21370
test_storage_iceberg_with_spark/test_multiple_iceberg_file.py	21315
test_create_query_constraints/test.py	21301
test_zookeeper_session_on_config_reload/test.py	21275
test_restart_with_unavailable_azure/test.py	21229
test_parallel_replicas_cluster_shadows_replicated_db/test.py	21226
test_attach_table_from_s3_plain_readonly/test.py	21125
test_storage_iceberg_with_spark/test_geometry_types.py	21104
test_keeper_client_config/test.py	21036
test_index_uncompressed_cache_zero_size/test.py	21021
test_replicated_merge_tree_replicated_db_ttl/test.py	21019
test_runtime_configurable_cache_size/test.py	21011
test_oom_canary/test.py	20923
test_system_zookeeper_watches/test.py	20897
test_s3_imds/test_simple.py	20725
test_storage_policies/test.py	20680
test_keeper_watches/test.py	20560
test_system_reconnect_zookeeper/test.py	20468
test_replicated_detach_table/test.py	20393
test_keeper_client/test.py	20310
test_storage_delta/test_azure_cluster.py	20189
test_keeper_read_during_close/test.py	20099
test_attach_table_normalizer/test.py	20024
test_async_connect_to_multiple_ips/test.py	20004
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
