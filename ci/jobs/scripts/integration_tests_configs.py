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
test_storage_s3_queue/test_6.py	1613975
test_storage_delta/test.py	1446199
test_storage_kafka/test_batch_fast.py	1382108
test_database_replicated_settings/test.py	1154675
test_replicated_database/test.py	1107258
test_storage_nats/test_nats_core.py	997631
test_max_bytes_ratio_before_external_order_group_by_for_server/test.py	994448
test_multiple_disks/test.py	962823
test_backup_restore_s3/test.py	956527
test_storage_rabbitmq/test.py	943274
test_keeper_session/test.py	934975
test_dictionaries_all_layouts_separate_sources/test_mongo.py	926855
test_backup_restore_new/test.py	861676
test_refreshable_mat_view/test.py	820257
test_dictionaries_redis/test.py	802992
test_storage_s3/test.py	765096
test_storage_s3_queue/test_system_stop.py	727443
test_storage_s3_queue/test_5.py	697632
test_postgresql_replica_database_engine/test_3.py	696414
test_distributed_load_balancing/test.py	656923
test_storage_azure_blob_storage/test.py	636570
test_restore_db_replica/test.py	603925
test_ttl_move/test.py	594060
test_storage_nats/test_nats_jet_stream.py	590281
test_dictionaries_all_layouts_separate_sources/test_clickhouse_remote.py	584146
test_dictionaries_all_layouts_separate_sources/test_clickhouse_local.py	581875
test_storage_kafka/test_system_stop.py	578780
test_dictionaries_all_layouts_separate_sources/test_mysql.py	574945
test_dictionaries_all_layouts_separate_sources/test_https.py	572651
test_dictionaries_all_layouts_separate_sources/test_http.py	570682
test_backup_restore_on_cluster/test_concurrency.py	567814
test_unknown_config_option/test.py	562917
test_storage_s3_queue/test_0.py	544757
test_async_load_databases/test.py	535859
test_mask_sensitive_info/test.py	504184
test_storage_s3_queue/test_2.py	500351
test_refreshable_mv/test.py	492746
test_storage_iceberg_with_spark/test_minmax_pruning.py	476962
test_storage_nats/test_system_stop.py	467940
test_named_collections/test.py	457791
test_storage_kafka/test_partition_affinity.py	455076
test_distributed_ddl/test.py	447824
test_backup_restore_on_cluster/test.py	426142
test_parallel_replicas_insert_select/test.py	423016
test_postgresql_replica_database_engine/test_1.py	421982
test_storage_rabbitmq/test_system_stop.py	421061
test_storage_iceberg_with_spark/test_cluster_table_function.py	414530
test_refreshable_mat_view_replicated/test.py	410944
test_database_iceberg/test.py	403519
test_concurrent_ttl_merges/test.py	401034
test_checking_s3_blobs_paranoid/test.py	390360
test_lost_part_during_startup/test.py	385610
test_storage_s3_queue/test_1.py	375213
test_database_delta/test.py	373082
test_merge_tree_s3/test.py	370555
test_dns_cache/test.py	365081
test_storage_iceberg_schema_evolution/test_evolved_schema_simple.py	362626
test_executable_table_function/test.py	361516
test_mysql_database_engine/test.py	358082
test_scheduler_io/test.py	350202
test_storage_kafka/test_batch_slow_1.py	348450
test_ttl_replicated/test.py	343526
test_kafka_bad_messages/test.py	343504
test_filesystem_cache/test.py	341092
test_storage_kafka/test_batch_slow_4.py	334626
test_named_collections_encrypted2/test.py	332664
test_storage_s3_queue/test_3.py	320924
test_mysql_protocol/test.py	317174
test_drop_is_lock_free/test.py	315804
test_dictionaries_dependency/test.py	315744
test_filesystem_split_cache/test.py	312332
test_database_glue/test.py	312017
test_dictionaries_ddl/test.py	303416
test_mysql57_database_engine/test.py	303134
test_ytsaurus/test_tables.py	300378
test_crash_log/test.py	296674
test_postgresql_replica_database_engine/test_2.py	296303
test_row_policy/test.py	291606
test_storage_iceberg_with_spark/test_partition_pruning.py	287602
test_storage_hdfs/test.py	286435
test_distributed_directory_monitor_split_batch_on_failure/test.py	283698
test_wasm_parser/test.py	281190
test_storage_kafka/test_batch_slow_5.py	273990
test_storage_iceberg_with_spark/test_expire_snapshots.py	271960
test_storage_iceberg_with_spark/test_system_iceberg_metadata.py	267410
test_dictionaries_all_layouts_separate_sources/test_file.py	266890
test_storage_kafka/test_compression_codec.py	258324
test_parallel_replicas_invisible_parts/test.py	255710
test_postpone_failed_tasks/test.py	255408
test_prometheus_protocols/test_evaluation.py	250752
test_backward_compatibility/test_aggregate_function_state.py	250583
test_s3_plain_rewritable/test.py	248498
test_create_handler/test.py	248148
test_storage_iceberg_schema_evolution/test_tuple_evolved_simple.py	245230
test_hedged_requests/test.py	244090
test_storage_bigquery/test.py	242364
test_refreshable_mv_no_multi_read/test.py	241415
test_table_db_num_limit/test.py	241283
test_storage_kafka/test_batch_slow_6.py	240826
test_cleanup_dir_after_bad_zk_conn/test.py	236604
test_keeper_ttl_nodes/test.py	235033
test_statistics_cache/test.py	233010
test_throttling/test.py	232465
test_backup_restore_on_cluster/test_cancel_backup.py	227874
test_postgresql_replica_database_engine/test_0.py	226307
test_polymorphic_parts/test.py	225380
test_insert_distributed_async_send/test.py	219788
test_storage_kafka/test_batch_slow_0.py	218500
test_storage_s3_queue/test_4.py	216430
test_azure_403_handling/test.py	216157
test_http_handlers_config/test.py	213758
test_parallel_replicas_over_distributed/test.py	211248
test_storage_iceberg_with_spark/test_position_deletes.py	205813
test_storage_postgresql/test.py	204391
test_implicit_index_upgrade/test.py	202896
test_storage_s3_queue/test_flush.py	201490
test_ai_functions/test.py	199665
test_storage_iceberg_schema_evolution/test_array_evolved_nested.py	198781
test_dictionaries_all_layouts_separate_sources/test_executable_hashed.py	196263
test_postgresql_database_engine/test.py	190690
test_storage_mysql/test.py	189944
test_broken_projections/test.py	189248
test_storage_mongodb/test.py	189186
test_dictionaries_all_layouts_separate_sources/test_mongo_uri.py	188919
test_scheduler_memory/test.py	187320
test_keeper_container_nodes/test.py	183656
test_backup_restore_new/test_cancel_backup.py	182446
test_storage_iceberg_no_spark/test_writes_statistics_by_minmax_pruning.py	181985
test_backward_compatibility/test_convert_ordinary.py	179211
test_grant_and_revoke/test_with_table_engine_grant.py	178546
test_replicated_mutations/test.py	176119
test_hive_query/test.py	175595
test_lost_part/test.py	175592
test_merge_tree_azure_blob_storage/test.py	174545
test_jbod_balancer/test.py	174319
test_storage_kerberized_kafka/test.py	174187
test_ytsaurus/test_dictionaries.py	174180
test_replicated_users/test.py	173729
test_executable_udf_async_metrics/test.py	173469
test_disk_over_web_server/test.py	173232
test_quorum_inserts/test.py	168584
test_storage_kafka/test_batch_slow_2.py	167406
test_system_logs/test_system_logs.py	167380
test_storage_kafka/test_kafka_zone_awareness.py	166160
test_replicated_fetches_bandwidth/test.py	165984
test_distributed_frozen_replica/test.py	165732
test_stop_insert_when_disk_close_to_full/test.py	164895
test_keeper_two_nodes_cluster/test.py	163599
test_plain_rewritable_backward_compatibility/test.py	163276
test_dictionaries_all_layouts_separate_sources/test_executable_cache.py	159013
test_storage_iceberg_with_spark/test_writes.py	157798
test_manipulate_statistics/test.py	155315
test_keeper_internal_secure/test.py	154786
test_keeper_snapshot_chunked_transfer/test.py	154502
test_cluster_discovery/test.py	153890
test_s3_plain_rewritable_rotate_tables/test.py	153860
test_drop_database_replica/test.py	152801
test_dictionaries_update_and_reload/test.py	152240
test_storage_iceberg_with_spark/test_remove_orphan_files.py	151637
test_parallel_replicas_custom_key_failover/test.py	150601
test_s3_aws_sdk_has_slightly_unreliable_behaviour/test.py	149132
test_encrypted_disk/test.py	147609
test_create_as_select_on_cluster_distributed/test.py	146653
test_text_index_upgrade/test.py	146007
test_rename_column/test.py	145088
test_database_backup/test.py	144777
test_storage_iceberg_with_spark/test_writes_mutate_delete.py	144738
test_keeper_zookeeper_converter/test.py	143464
test_prometheus_protocols/test_different_table_engines.py	141550
test_restore_replica/test.py	141480
test_keeper_map/test.py	140700
test_transactions/test.py	139396
test_system_clusters_actual_information/test.py	137734
test_log_query_probability/test.py	137672
test_storage_iceberg_disks/test.py	134944
test_refreshable_mv_skip_old_temp_table_ddls/test.py	133133
test_scheduler_query/test.py	132077
test_kafka_bad_messages/test_mv_target_missing.py	131787
test_storage_url/test.py	130618
test_system_logs_recreate/test.py	129868
test_distributed_index_analysis/test.py	128188
test_storage_kafka/test_keeper_session_loss_direct_read.py	127344
test_ddl_worker_replicas/test.py	127111
test_attach_without_fetching/test.py	126762
test_quota/test.py	124224
test_parallel_replicas_custom_key_load_balancing/test.py	122016
test_recompression_ttl/test.py	121346
test_distributed_ddl_parallel/test.py	121214
test_merges_memory_limit/test.py	121152
test_postgresql_ssl/test.py	120676
test_MemoryTracking/test.py	120405
test_disk_access_storage/test.py	120074
test_version_update_after_mutation/test.py	118871
test_recovery_replica/test.py	118463
test_merge_tree_hdfs/test.py	118064
test_storage_iceberg_schema_evolution/test_tuple_evolved_nested.py	117956
test_storage_iceberg_with_spark/test_writes_create_partitioned_table.py	115810
test_disk_configuration/test.py	114872
test_storage_kafka/test_produce_http_interface.py	114664
test_named_collections_encrypted2/test_integr.py	113908
test_mutations_with_merge_tree/test.py	112989
test_storage_hudi/test.py	111252
test_reloading_storage_configuration/test.py	110453
test_storage_iceberg_with_spark/test_manifest_compaction.py	108990
test_replicated_user_defined_functions/test.py	108071
test_partition/test.py	107990
test_zookeeper_config/test_secure.py	107818
test_system_merges/test.py	107532
test_database_remote/test.py	106822
test_backup_restore_on_cluster_with_checksum_data_file_name/test.py	106040
test_migration_deduplication_hash/test.py	105730
test_alter_settings_or_comment_on_cluster/test.py	104527
test_database_disk_setting/test.py	104266
test_backup_restore_azure_blob_storage/test.py	103570
test_dictionaries_postgresql/test.py	102695
test_alter_moving_garbage/test.py	102414
test_executable_dictionary/test.py	102120
test_s3_table_functions/test.py	101158
test_distributed_inter_server_secret/test.py	100491
test_allowed_url_from_config/test.py	100473
test_dictionary_lazy_load/test.py	99598
test_backup_restore_keeper_map/test.py	99568
test_merge_tree_load_parts/test.py	99194
test_scheduler_cpu/test.py	97314
test_drop_replica_with_auxiliary_zookeepers/test.py	96632
test_executable_user_defined_function/test.py	95914
test_dictionaries_mysql/test.py	95718
test_storage_delta_disks/test.py	95670
test_modify_engine_on_restart/test_ordinary.py	95517
test_distributed_plan_replicated_merge_tree/test.py	95191
test_globs_in_filepath/test.py	94136
test_settings_profile/test.py	93936
test_s3_cluster/test.py	93582
test_keeper_reconfig_replace_leader_in_one_command/test.py	92351
test_arrowflight_interface/test_sql_server.py	92204
test_cluster_discovery/test_dynamic_clusters.py	91954
test_keeper_back_to_back/test.py	91937
test_storage_iceberg_with_spark/test_query_condition_cache.py	91648
test_rocksdb_options/test.py	91566
test_azure_blob_storage_plain_rewritable/test.py	90448
test_executable_udf_profile_events/test.py	90183
test_cluster_discovery/test_auxiliary_keeper.py	90112
test_system_start_stop_listen/test.py	90065
test_check_table/test.py	88731
test_random_inserts/test.py	88376
test_storage_iceberg_with_spark/test_metadata_file_format_with_uuid.py	88239
test_distributed_ddl/test_replicated_alter.py	88148
test_restore_replica_metadata_version/test.py	88100
test_keeper_incorrect_config/test.py	87944
test_keeper_remove_rejoin_leader/test.py	87725
test_keeper_password/test.py	87360
test_keeper_three_nodes_two_alive/test.py	87092
test_storage_iceberg_with_spark/test_metadata_file_selection.py	86590
test_storage_iceberg_with_spark/test_writes_mutate_update.py	86132
test_scheduler_cpu_preemptive/test.py	85310
test_storage_iceberg_with_spark/test_schema_evolution_with_time_travel.py	85302
test_format_schema_source/test.py	85256
test_s3_cluster_restart/test.py	85071
test_backup_source_grants/test.py	84916
test_server_reload/test.py	84642
test_storage_s3_queue/test_parallel_inserts.py	84596
test_modify_engine_on_restart/test.py	84311
test_backup_restore_on_cluster/test_disallow_concurrency.py	84292
test_access_control_with_custom_setup/test.py	83790
test_remote_blobs_naming/test_backward_compatibility.py	83432
test_restore_external_engines/test.py	83350
test_system_metrics/test.py	83216
test_s3_table_function_with_http_proxy/test.py	83090
test_ddl_create_then_alter_offline_replica/test.py	82790
test_s3_table_function_with_https_proxy/test.py	82593
test_zookeeper_send_window_broken_promise/test.py	82424
test_jbod_ha/test.py	82322
test_storage_iceberg_with_spark/test_schema_inference.py	81757
test_keeper_opentelemetry_tracing/test.py	81676
test_refreshable_mv_watch_fault/test.py	81602
test_system_flush_logs/test.py	81261
test_paimon_incremental_read/test.py	81207
test_store_cleanup/test.py	80336
test_rmv_access_denied_on_rename_race/test.py	80216
test_always_fetch_merged/test.py	80176
test_hedged_requests_parallel/test.py	80085
test_replicated_merge_tree_compatibility/test.py	79549
test_replicated_database_interserver_host/test.py	79491
test_replication_credentials/test.py	79303
test_clickhouse_server_wait_server_pool/test.py	79161
test_https_replication/test.py	79067
test_consistant_parts_after_move_partition/test.py	78876
test_query_runner/test.py	77992
test_dictionaries_dependency_xml/test.py	77866
test_s3_credentials_hardening/test.py	77079
test_client_auto_secure_port/test.py	76656
test_keeper_force_recovery/test.py	75310
test_file_schema_inference_cache/test.py	75203
test_mark_cache_profile_events/test.py	74998
test_storage_kafka_sasl/test.py	74949
test_parallel_replicas_custom_key/test.py	74585
test_keeper_disks/test.py	74204
test_storage_kafka/test_avro_schema_registry.py	74162
test_log_family_hdfs/test.py	73123
test_replicated_merge_tree_encryption_codec/test.py	72987
test_database_catalog_shutdown_system_logs/test.py	72954
test_index_filename_upgrade/test.py	72888
test_keeper_ttl_nodes/test_disabled.py	72679
test_replicated_table_attach/test.py	72307
test_storage_iceberg_with_spark/test_writes_create_table.py	72062
test_user_memory_tracker_log_drift/test.py	71996
test_inserts_with_keeper_retries/test.py	71953
test_storage_iceberg_schema_evolution/test_array_evolved_with_struct.py	71733
test_multi_access_storage_role_management/test.py	71506
test_keeper_persistent_watches/test.py	70714
test_replicated_database_cluster_groups/test.py	70290
test_storage_s3_queue/test_sts_smoke.py	70182
test_server_overload/test.py	69888
test_replicated_database_recover_digest_mismatch/test.py	69520
test_sparsity_exact_num_defaults_compat/test.py	69389
test_storage_iceberg_schema_evolution/test_evolved_schema_complex.py	68678
test_insert_into_distributed/test.py	68640
test_merge_tree_s3_failover/test.py	68502
test_default_compression_codec/test.py	68470
test_keeper_container_nodes/test_disabled.py	68352
test_keeper_nodes_remove/test.py	67862
test_backward_compatibility/test_aggregate_function_state_contingency_functions.py	67408
test_storage_iceberg_with_trino/test.py	67078
test_redirect_url_storage/test.py	67040
test_replicated_merge_tree_wait_on_shutdown/test.py	66528
test_sharding_key_from_default_column/test.py	66465
test_named_collections_if_exists_on_cluster/test.py	65646
test_storage_iceberg_with_spark/test_iceberg_snapshot_reads.py	64850
test_storage_kafka/test_poll_timeout_after_assignment.py	64316
test_postgresql_remote_host_filter/test.py	63972
test_keeper_4lw_reconfiguration/test.py	63949
test_parallel_replicas_snapshot_from_initiator/test.py	63716
test_async_insert_memory/test.py	63628
test_alter_on_mixed_type_cluster/test.py	62406
test_graphite_merge_tree/test.py	62189
test_graphite_merge_tree_typed/test.py	61480
test_backup_restore_on_cluster/test_huge_concurrent_restore.py	61250
test_storage_iceberg_with_spark/test_explicit_metadata_file.py	61070
test_insert_distributed_load_balancing/test.py	61062
test_azure_blob_storage_native_copy/test.py	60738
test_lightweight_updates/test.py	60131
test_background_operations_config/test.py	60034
test_host_regexp_multiple_ptr_records/test.py	60008
test_parallel_replicas_distributed_skip_shards/test.py	59786
test_group_array_element_size/test.py	59527
test_packed_io/test.py	59500
test_limited_replicated_fetches/test.py	59184
test_keeper_max_append_byte_size/test.py	59175
test_keeper_auth/test.py	59088
test_settings_constraints/test.py	59058
test_dictionaries_replace/test.py	59016
test_keeper_reconfig_remove_many/test.py	58906
test_phantom_parts_in_mutations/test.py	58841
test_distributed_format/test.py	58835
test_storage_iceberg_with_spark/test_writes_schema_evolution.py	58014
test_table_function_mongodb/test.py	57977
test_backup_restore_new/test_shutdown_wait_backup.py	57930
test_keeper_snapshot_small_distance/test.py	57851
test_executable_pool_udf_profile_events/test.py	57696
test_create_union_system_log_tables/test.py	57644
test_storage_iceberg_with_spark/test_delete_files.py	57614
test_backward_compatibility/test_block_marshalling.py	57374
test_storage_redis/test.py	57260
test_allow_feature_tier/test.py	57255
test_no_merges_volume_ttl/test.py	57196
test_zookeeper_config_load_balancing/test.py	57096
test_distributed_ddl_password/test.py	56972
test_delayed_replica_failover/test.py	56910
test_grant_and_revoke/test_without_table_engine_grant.py	56910
test_send_request_to_leader_replica/test.py	56858
test_replicated_merge_tree_encrypted_disk/test.py	56705
test_backup_restore_on_cluster/test_two_shards_two_replicas.py	56696
test_storage_iceberg_concurrent/test_concurrent_reads.py	56530
test_backward_compatibility/test_aggregate_function_state_tuple_return_type.py	56482
test_system_detached_tables/test.py	56431
test_https_replication/test_change_ip.py	55684
test_attach_partition_using_copy/test.py	54928
test_lightweight_updates_compatibility/test.py	54882
test_ddl_on_cluster_stop_waiting_for_offline_hosts/test.py	54584
test_keeper_block_acl/test.py	54482
test_async_insert_adaptive_busy_timeout/test.py	54305
test_keeper_nodes_add/test.py	54056
test_mysql_kill_query/test.py	53842
test_named_collections_encrypted/test.py	53840
test_old_parts_finally_removed/test.py	53484
test_system_ddl_worker_queue/test.py	53344
test_storage_iceberg_with_spark/test_format_version_upgrade.py	53308
test_warning_broken_tables/test.py	53251
test_string_aggregation_compatibility/test.py	52830
test_storage_iceberg_with_spark/test_async_metadata_refresh.py	52802
test_read_only_table/test.py	52705
test_keeper_mntr_pressure/test.py	52678
test_user_valid_until/test.py	52159
test_database_iceberg_lakekeeper_catalog/test.py	52137
test_storage_iceberg_schema_evolution/test_array_map_evolved_with_struct.py	52008
test_quorum_inserts_parallel/test.py	51894
test_memory_limit_observer/test.py	51849
test_storage_kafka/test_intent_sizes.py	51431
test_disabled_access_control_improvements/test_row_policy.py	51412
test_replicated_merge_tree_with_auxiliary_zookeepers/test.py	51133
test_storage_s3/test_sts.py	51050
test_storage_iceberg_with_spark/test_writes_with_partitioned_table.py	50959
test_nullable_tuple_subcolumns/test.py	50850
test_backward_compatibility/test_bucketed_map_order.py	50844
test_storage_numbers/test.py	50822
test_replace_partition/test.py	50768
test_user_directories/test.py	50648
test_storage_iceberg_with_spark/test_read_in_order.py	50470
test_mutations_in_partitions_of_merge_tree/test.py	49956
test_tmp_policy/test.py	49906
test_version_update/test.py	49875
test_reload_auxiliary_zookeepers/test.py	49819
test_replicated_access/test.py	49754
test_prometheus_protocols/test_compliance.py	49628
test_max_suspicious_broken_parts_replicated/test.py	49592
test_storage_iceberg_with_spark/test_bucket_partition_pruning.py	49442
test_consistent_parts_after_clone_replica/test.py	49429
test_storage_alias_replicated/test.py	49360
test_keeper_map_retries/test.py	49266
test_backward_compatibility/test_pr_protocol_with_stream_id.py	49263
test_temporary_data_in_cache/test.py	49184
test_concurrent_queries_restriction_by_query_kind/test.py	48998
test_fetch_partition_should_reset_mutation/test.py	48502
test_storage_kafka/test_schema_registry_skip_bytes.py	48382
test_backward_compatibility/test_parallel_replicas_protocol.py	48362
test_database_iceberg_nessie_catalog/test.py	48272
test_encrypted_disk_replication/test.py	47988
test_keeper_readahead/test.py	47946
test_kafka_bad_messages/test_1.py	47728
test_fetch_partition_from_auxiliary_zookeeper/test.py	47616
test_config_substitutions/test.py	47611
test_backup_restore_on_cluster/test_slow_rmt.py	47593
test_storage_kafka/test_zookeeper_locks.py	47556
test_matview_union_replicated/test.py	47528
test_statistics_minmax_upgrade/test.py	47259
test_https_s3_table_function_with_http_proxy_no_tunneling/test.py	46767
test_grpc_protocol/test.py	46615
test_distributed_insert_backward_compatibility/test.py	46562
test_on_cluster_timeouts/test.py	46136
test_reload_clusters_config/test.py	45946
test_fetch_partition_with_outdated_parts/test.py	45883
test_backup_restore_on_cluster_s3_credentials/test.py	45846
test_keeper_znode_time/test.py	45796
test_backup_restore/test.py	45670
test_zookeeper_config/test.py	45619
test_keeper_as_server/test.py	45601
test_non_default_compression/test.py	45358
test_settings_constraints_distributed/test.py	45320
test_race_condition_for_replicated_merge_tree/test.py	45291
test_join_set_family_s3/test.py	45016
test_drop_replica/test.py	44784
test_replicated_s3_zero_copy_drop_partition/test.py	44699
test_http_failover/test.py	44423
test_backup_restore_on_cluster/test_different_versions.py	44322
test_sql_user_defined_functions_on_cluster/test.py	44192
test_disks_app_func/test.py	44004
test_postgresql_kill_query/test.py	43747
test_part_uuid/test.py	43699
test_undrop_query/test.py	43694
test_variant_escaping_merge_tree_compatibility/test.py	43291
test_concurrent_threads_soft_limit/test.py	42810
test_zookeeper_connection_log/test.py	42750
test_ldap_external_user_directory/test.py	42482
test_replicated_merge_tree_s3/test.py	42475
test_keeper_snapshot_chunked_transfer/test_concurrent.py	41986
test_storage_s3/test_invalid_env_credentials.py	41864
test_keeper_multinode_simple/test.py	41799
test_storage_kafka/test_batch_slow_7.py	41450
test_keeper_four_word_command/test.py	41324
test_replicated_fetches_min_part_level/test.py	41317
test_dictionary_ddl_on_cluster/test.py	40524
test_attach_with_different_projections_or_indices/test.py	40508
test_ssl_cert_authentication/test.py	40427
test_dictionaries_redis/test_long.py	40395
test_dictionaries_config_reload/test.py	40302
test_keeper_feature_flags_config/test.py	40173
test_force_drop_table/test.py	40114
test_cross_replication/test.py	39905
test_keeper_nodes_move/test.py	39775
test_disabled_access_control_improvements/test_users_without_row_policies_can_read_rows.py	39612
test_arrowflight_interface/test.py	39553
test_insert_over_http_query_log/test.py	39552
test_zookeeper_config/test_password.py	39494
test_part_loading_tree_rollback/test.py	39452
test_arrowflight_storage/test.py	39382
test_keeper_session_refuse_stale_server/test.py	39036
test_executable_user_defined_functions_config_reload/test.py	39018
test_backup_restore_storage_policy/test.py	39014
test_file_cluster/test.py	38736
test_modify_engine_on_restart/test_storage_policies.py	38673
test_storage_iceberg_with_spark_cache/test_metadata_cache.py	38591
test_alter_database_on_cluster/test.py	38518
test_ddl_worker_stale_task_name/test.py	38440
test_db_ordinary_deprecated_warning/test.py	38418
test_parts_delete_zookeeper/test.py	38146
test_config_decryption/test_wrong_settings.py	37877
test_backward_compatibility/test_ip_types_binary_compatibility.py	37836
test_keeper_broken_logs/test.py	37817
test_storage_iceberg_with_spark/test_metadata_file_selection_from_version_hint.py	37780
test_storage_delta_shuffles/test.py	37489
test_storage_iceberg_interoperability_azure/test_interoperability.py	37488
test_parallel_replicas_failover/test.py	37354
test_keeper_force_recovery_single_node/test.py	37280
test_alternative_keeper_config/test.py	37262
test_storage_iceberg_schema_evolution/test_map_evolved_nested.py	37044
test_force_deduplication/test.py	36948
test_zero_copy_drop_table_with_leftover/test.py	36750
test_database_hms/test.py	36696
test_database_hms/test_ttransport_exception_reproduction.py	36578
test_storage_delta/test_cdf.py	36507
test_odbc_interaction/test.py	36420
test_storage_iceberg_with_spark/test_optimize.py	36336
test_keeper_max_request_size/test.py	36261
test_keeper_snapshots/test.py	36159
test_transposed_metric_log/test.py	36119
test_atomic_drop_table/test.py	36042
test_dictionaries_select_all/test.py	35983
test_distributed_ddl_on_cross_replication/test.py	35967
test_reload_client_certificate/test.py	35886
test_keeper_log_gap_before_committed/test.py	35837
test_s3_storage_conf_proxy/test.py	35791
test_ddl_alter_query/test.py	35774
test_reload_zookeeper/test.py	35771
test_move_partition_to_volume_async/test.py	35761
test_s3_cluster_insert_select/test.py	35709
test_keeper_reconfig_replace_leader/test.py	35627
test_prometheus_protocols/test_upgrade_from_prealpha.py	35537
test_keeper_reconfig_remove/test.py	35527
test_storage_iceberg_with_spark/test_file_stats_logging.py	35522
test_storage_iceberg_no_spark/test_writes_rename_column.py	35492
test_http_connection_socket_buffer_settings/test.py	35456
test_replicated_fetches_timeouts/test.py	35429
test_backward_compatibility/test_adaptive_codec.py	34747
test_prometheus_endpoint/test.py	34691
test_session_log/test.py	34682
test_attach_partition_with_large_destination/test.py	34654
test_startup_scripts/test.py	34635
test_profile_max_sessions_for_user/test.py	34626
test_dremio_engine/test.py	34619
test_storage_iceberg_with_spark/test_explanation.py	34592
test_remove_stale_moving_parts/test.py	34576
test_keeper_dynamic_settings/test.py	34496
test_max_suspicious_broken_parts/test.py	34364
test_keeper_s3_snapshot/test.py	34325
test_mutations_with_projection/test.py	34201
test_distributed_async_insert_for_node_changes/test.py	34184
test_postgresql_protocol/test.py	33960
test_select_access_rights/test_from_system_tables.py	33938
test_modify_engine_on_restart/test_mv.py	33577
test_access_control_on_cluster/test.py	33572
test_keeper_snapshot_on_exit/test.py	33538
test_restart_server/test.py	33387
test_acme_tls/test_multi_node.py	33268
test_storage_azure_blob_storage/test_cluster.py	33154
test_disk_checker/test.py	33105
test_parallel_replicas_protocol/test.py	33102
test_replication_without_zookeeper/test.py	32830
test_distributed_respect_user_timeouts/test.py	32646
test_zookeeper_fallback_session/test.py	32561
test_force_restore_data_flag_for_keeper_dataloss/test.py	32364
test_keeper_snapshot_rotation_race/test.py	32053
test_parallel_replicas_alias_columns/test.py	31970
test_user_query_log_config_validation/test.py	31804
test_hot_reload_storage_policy/test.py	31548
test_storage_iceberg_interoperability_local/test_interoperability.py	31528
test_rocksdb_read_only/test.py	31522
test_server_startup_and_shutdown_logs/test.py	31389
test_suggestions/test.py	31370
test_ddl_worker_non_leader/test.py	31348
test_distributed_structure_fetch/test.py	31246
test_modify_engine_on_restart/test_zk_path_exists.py	31193
test_reloading_settings_from_users_xml/test.py	31108
test_mongodb_kill_query/test.py	30755
test_ttl_multilevel_group_by/test.py	30734
test_asynchronous_metrics_pk_bytes_fields/test.py	30635
test_materialize_projections_on_merge/test.py	30488
test_analyzer_compatibility/test.py	30478
test_webassembly_udf/test.py	30423
test_search_orphaned_parts/test.py	30396
test_compressed_marks_restart/test.py	30323
test_acme_tls/test_single_node.py	30234
test_access_cache_recompute_coalescing/test.py	30216
test_keeper_persistent_log_multinode/test.py	30203
test_compression_nested_columns/test.py	30189
test_dictionaries_wait_for_load/test.py	30185
test_distributed_plan_worker_exchange_port/test.py	29933
test_secure_socket/test.py	29883
test_keeper_reconfig_add/test.py	29876
test_backward_compatibility/test_functions.py	29795
test_cache_bypass_on_disk_failure/test.py	29769
test_concurrent_part_removal_threshold_for_remote_disk/test.py	29675
test_disabled_access_control_improvements/test_select_from_system_tables.py	29636
test_storage_s3_queue/test_file_iterator_ttl.py	29607
test_user_defined_object_persistence/test.py	29522
test_merge_tree_s3_with_cache/test.py	29517
test_play_reconcile_startup/test.py	29416
test_modify_engine_on_restart/test_args.py	29283
test_storage_iceberg_with_spark/test_column_names_with_dots.py	29254
test_keeper_azure_s3_plain/test.py	29228
test_sync_replica_on_cluster/test.py	29228
test_parallel_replicas_all_marks_read/test.py	29222
test_parallel_replicas_no_replicas/test.py	29205
test_disable_insertion_and_mutation/test.py	29202
test_format_avro_confluent/test.py	29091
test_sqlite_kill_query/test.py	29078
test_modify_engine_on_restart/test_unusual_path.py	28947
test_keeper_unpreprocessed_logs_livelock/test.py	28870
test_shutdown_wait_unfinished_queries/test.py	28865
test_backup_restore_s3/test_throttling.py	28860
test_drop_if_empty/test.py	28852
test_rabbitmq_malicious_broker/test.py	28790
test_ddl_worker_with_loopback_hosts/test.py	28787
test_log_family_s3/test.py	28602
test_truncate_database/test_distributed.py	28600
test_permissions_drop_replica/test.py	28460
test_parallel_replicas_skip_inactive_replicas/test.py	28423
test_dictionary_asynchronous_metrics/test.py	28387
test_validate_only_initial_alter_query/test_replicated_database.py	28367
test_parallel_replicas_skip_inactive_replicas_all_groups/test.py	28342
test_storage_iceberg_with_spark/test_writes_field_ids_spark_read.py	28222
test_system_queries/test.py	28209
test_match_process_uid_against_data_owner/test.py	28138
test_dictionary_allow_read_expired_keys/test_dict_get_or_default.py	28121
test_keeper_follower_metrics/test.py	28074
test_placement_info/test.py	28036
test_system_logs_hostname/test_replicated.py	27943
test_paimon_rest_catalog/test.py	27819
test_insert_into_distributed_sync_async/test.py	27811
test_keeper_raft_cert_reload/test.py	27657
test_covered_by_broken_exists/test.py	27596
test_compatibility_merge_tree_settings/test.py	27546
test_experimental_codec_config_default/test.py	27533
test_dictionary_allow_read_expired_keys/test_dict_get.py	27527
test_filesystem_layout/test.py	27420
test_reset_ddl_worker/test.py	27418
test_filesystem_cache_eviction_metrics/test.py	27400
test_dictionary_allow_read_expired_keys/test_default_reading.py	27289
test_storage_iceberg_with_spark/test_minmax_pruning_with_null.py	27278
test_keeper_persistent_log/test.py	27187
test_storage_iceberg_with_spark/test_writes_from_zero.py	27088
test_fix_metadata_version/test.py	26946
test_keeper_remove_acl/test.py	26818
test_extreme_deduplication/test.py	26704
test_replica_is_active/test.py	26637
test_part_log_table/test.py	26487
test_replicated_access/test_invalid_entity.py	26443
test_backup_log/test.py	26414
test_cleanup_after_start/test.py	26386
test_bind_host/test.py	26357
test_executable_udf_names_in_system_query_log/test.py	26290
test_s3_access_headers/test.py	26223
test_check_table_name_length_2/test.py	26128
test_settings_constraints_distributed_ddl/test.py	26073
test_ddl_worker_retry_when_dropping_db_failed/test.py	26051
test_mutation_fetch_fallback/test.py	25919
test_detached_parts_metrics/test.py	25761
test_insert_into_distributed_through_materialized_view/test.py	25673
test_storage_iceberg_no_spark/test_iceberg_history_large_summary.py	25656
test_disabled_mysql_server/test.py	25504
test_settings_from_server/test.py	25480
test_server_start_and_ip_conversions/test.py	25386
test_storage_iceberg_with_spark/test_writes_complex_type.py	25264
test_keeper_three_nodes_start/test.py	24836
test_alter_comment_on_cluster/test.py	24797
test_no_password_existing_user/test.py	24782
test_userspace_page_cache/test.py	24599
test_storage_iceberg_schema_evolution/test_correct_column_mapper_is_chosen.py	24584
test_parallel_replicas_insert_select_coordinator_reuse/test.py	24567
test_distributed_ddl_on_database_cluster/test.py	24551
test_access_for_functions/test.py	24534
test_max_rows_to_read_leaf_with_view/test.py	24505
test_table_function_redis/test.py	24448
test_topk_alpha_map_compatibility/test.py	24432
test_keeper_leader_metrics/test.py	24430
test_default_database_on_cluster/test.py	24402
test_old_versions/test.py	24382
test_default_compression_in_mergetree_settings/test.py	24281
test_alter_settings_on_cluster/test.py	24204
test_parallel_replicas_increase_error_count/test.py	24142
test_cow_policy/test.py	24093
test_log_lz4_streaming/test.py	24028
test_optimize_on_insert/test.py	24013
test_broken_part_during_merge/test.py	23968
test_threadpool_readers/test.py	23938
test_external_cluster/test.py	23932
test_zero_copy_lock_leak/test.py	23845
test_azure_blob_storage_listobjects_prefix/test.py	23844
test_zero_copy_expand_macros/test.py	23690
test_wrong_db_or_table_name/test.py	23657
test_mutations_hardlinks/test.py	23628
test_asynchronous_metric_log_table/test.py	23595
test_early_memory_limit_exception/test.py	23471
test_ddl_config_hostname/test.py	23344
test_intersecting_parts/test.py	23333
test_backward_compatibility/test_aggregation_with_out_of_order_buckets.py	23278
test_keeper_mntr_data_size/test.py	23206
test_keeper_snapshots_multinode/test.py	23181
test_totp_auth/test_totp.py	23166
test_storage_iceberg_no_spark/test_cluster_partition_pruning_reads.py	23150
test_move_partition_to_disk_on_cluster/test.py	23132
test_keeper_restore_from_snapshot/test_disk_s3.py	23104
test_recovery_time_metric/test.py	22942
test_storage_s3/test_parquet_prewhere.py	22926
test_azure_disk_unreachable/test.py	22913
test_cluster_discovery/test_password.py	22826
test_broken_tmp_txn_version_startup/test.py	22820
test_peak_memory_usage/test.py	22817
test_profile_events_s3/test.py	22790
test_truncate_database/test_replicated.py	22750
test_parameterized_view/test.py	22723
test_external_http_authenticator/test.py	22718
test_replicated_database_alter_modify_order_by/test.py	22710
test_deduplicated_attached_part_rename/test.py	22688
test_aliases_in_default_expr_not_break_table_structure/test.py	22476
test_paimon_metadata_files_cache/test.py	22414
test_replicated_merge_tree_thread_schedule_timeouts/test.py	22391
test_max_authentication_methods_per_user/test.py	22371
test_s3_low_cardinality_right_border/test.py	22298
test_replicated_database_with_auxiliary_zookeepers/test.py	22297
test_backward_compatibility/test_nullable_sparse_compatibility.py	22285
test_shard_level_const_function/test.py	22266
test_storage_iceberg_schema_evolution/test_full_drop.py	22237
test_s3_storage_conf_new_proxy/test.py	22197
test_format_schema_on_server/test.py	22172
test_parallel_replicas_local_replica_forced_inactive/test.py	22060
test_ldap_follow_referrals/test.py	22049
test_limit_by_transform_kill_query/test.py	22024
test_executable_user_defined_function/test_system_table.py	21873
test_point_in_polygon_cache_size/test.py	21808
test_prometheus_before_tables/test.py	21766
test_storage_delta/test_imds.py	21763
test_prefer_global_in_and_join/test.py	21743
test_prometheus_protocols/test_write_read.py	21640
test_executable_user_defined_function_lifetime_reload/test.py	21542
test_storage_iceberg_no_spark/test_read_in_order_with_pyiceberg.py	21515
test_zookeeper_session_on_config_reload/test.py	21510
test_merge_tree_empty_parts/test.py	21468
test_http_limits/test_hard_limit.py	21375
test_sql_roles_for_xml_users/test.py	21294
test_create_query_constraints/test.py	21239
test_restart_with_unavailable_azure/test.py	21226
test_storage_iceberg_with_spark/test_multiple_iceberg_file.py	21212
test_groupBitmapAnd_on_distributed/test_groupBitmapAndState_on_distributed_table.py	21194
test_parallel_replicas_cluster_shadows_replicated_db/test.py	21146
test_storage_iceberg_with_spark_cache/test_filesystem_cache.py	21133
test_replicated_merge_tree_replicated_db_ttl/test.py	21105
test_attach_table_from_s3_plain_readonly/test.py	21099
test_keeper_client_config/test.py	21036
test_storage_iceberg_with_spark/test_geometry_types.py	20992
test_runtime_configurable_cache_size/test.py	20977
test_oom_canary/test.py	20974
test_system_zookeeper_watches/test.py	20931
test_index_uncompressed_cache_zero_size/test.py	20812
test_storage_policies/test.py	20736
test_s3_imds/test_simple.py	20657
test_groupBitmapAnd_on_distributed/test.py	20640
test_keeper_watches/test.py	20569
test_system_reconnect_zookeeper/test.py	20466
test_replicated_detach_table/test.py	20409
test_storage_iceberg_no_spark/test_local_path_traversal.py	20366
test_keeper_client/test.py	20224
test_introspection_port/test.py	20132
test_storage_iceberg_no_spark/test_writes_multiple_files.py	20082
test_keeper_read_during_close/test.py	20059
test_async_connect_to_multiple_ips/test.py	20057
test_interserver_dns_retires/test.py	20045
test_storage_delta/test_azure_cluster.py	20005
test_attach_table_normalizer/test.py	19964
test_union_header/test.py	19807
test_backward_compatibility/test_rocksdb_upgrade.py	19765
test_backward_compatibility/test.py	19732
test_keeper_catchup_response_queue/test.py	19709
test_insert_deduplication_version_guard/test.py	19680
test_backward_compatibility/test_aggregate_fixed_key.py	19660
test_allowed_client_hosts/test.py	19636
test_mutations_analyzer_override/test.py	19629
test_replica_can_become_leader/test.py	19622
test_os_thread_nice_value/test.py	19566
test_geojson_format/test.py	19542
test_storage_iceberg_no_spark/test_writes_multiple_threads.py	19496
test_create_dictionary_in_startup_script/test.py	19488
test_insert_distributed_async_extra_dirs/test.py	19446
test_jbod_load_balancing/test.py	19338
test_role/test_replicated_ddl_current_roles.py	19322
test_replicated_engine_arguments/test.py	19274
test_limit_materialized_view_count/test.py	19048
test_thread_pool_free_size_shutdown/test.py	18970
test_replicated_merge_tree_config/test.py	18935
test_distributed_over_distributed/test.py	18890
test_config_reloader_interval/test.py	18856
test_distributed_async_insert_batch_recovery/test.py	18808
test_storage_iceberg_with_spark/test_types.py	18786
test_distributed_storage_configuration/test.py	18725
test_system_logs_comment/test.py	18678
test_dictionary_allow_read_expired_keys/test_default_string.py	18585
test_distributed_broken_files_stat/test.py	18538
test_merge_table_over_distributed/test.py	18516
test_temporary_data/test.py	18494
test_storage_dict/test.py	18488
test_enable_user_name_access_type/test.py	18476
test_japanese_tokenizer/test.py	18438
test_plain_rewr_legacy_layout/test.py	18371
test_alter_update_cast_keep_nullable/test.py	18257
test_access_denied_hint_sanitized/test.py	18214
test_storage_s3_queue/test_file_iterator_lost_lock.py	18106
test_config_decryption/test_zk_secure.py	18037
test_ssh/test.py	18010
test_check_table_name_length/test.py	18008
test_remote_storage_engine_attach/test.py	17963
test_scram_sha256_password_with_replicated_zookeeper_replicator/test.py	17786
test_reader_executor_page_cache/test.py	17708
test_timezone_config/test.py	17585
test_reload_max_table_size_to_drop/test.py	17539
test_storage_iceberg_no_spark/test_expire_snapshots_history.py	17536
test_async_logger_metrics/test.py	17510
test_backward_compatibility/test_memory_bound_aggregation.py	17466
test_storage_gcp_auth/test.py	17466
test_replicated_parse_zk_metadata/test.py	17455
test_storage_iceberg_with_spark/test_restart_broken_s3.py	17354
test_fetch_memory_usage/test.py	17348
test_attach_backup_from_s3_plain/test.py	17283
test_tcp_hello_string_limits/test.py	17142
test_drop_no_local_path/test.py	17131
test_metdata_cache_memory_leak/test.py	17106
test_arrowflight_interface/test_prepared_statement_ttl.py	16986
test_codec_encrypted/test.py	16894
test_multiple_authentication_methods/test.py	16868
test_cluster_all_replicas/test.py	16860
test_config_decryption/test_zk.py	16824
test_max_temporary_data_size_on_disk/test.py	16809
test_git_import/test.py	16756
test_keeper_availability_zone/test.py	16685
test_disks_app_other_disk_types/test.py	16674
test_keeper_restore_from_snapshot/test.py	16614
test_distributed_default_database/test.py	16600
test_backward_compatibility/test_const_node_optimization.py	16579
test_aggregation_memory_efficient/test.py	16554
test_replicating_constants/test.py	16538
test_refreshable_mv_keeper_loss/test.py	16530
test_trace_log_build_id/test.py	16510
test_shutdown_static_destructor_failure/test.py	16494
test_password_constraints/test.py	16486
test_config_not_overriding_args/test.py	16458
test_prometheus_protocols/test_prometheus_query_log.py	16436
test_log_levels_update/test.py	16413
test_failed_async_inserts/test.py	16394
test_distributed_system_query/test.py	16370
test_keeper_java_client/test.py	16347
test_kerberos_auth/test.py	16317
test_azure_workload_identity/test.py	16256
test_keeper_slow_connection_log/test.py	16192
test_storage_s3_intelligent_tier/test.py	16168
test_storage_delta/test_sts.py	16154
test_prometheus_protocols/test_insert_select.py	16122
test_storage_iceberg_with_spark/test_partition_pruning_with_subquery_set.py	16119
test_keeper_sanitizer_logs/test.py	16118
test_prometheus_protocols/test_query_cache.py	16070
test_keeper_ipv4_fallback/test.py	15938
test_user_ip_restrictions/test.py	15850
test_storage_iceberg_with_spark/test_manifest_data_path_security.py	15830
test_composable_protocols/test.py	15739
test_webterminal/test.py	15738
test_block_structure_mismatch/test.py	15662
test_config_decryption/test_wrong_settings_zk.py	15624
test_input_format_parallel_parsing_memory_tracking/test.py	15600
test_reload_certificate/test.py	15590
test_mysql_protocol/test_kill_query.py	15538
test_read_temporary_tables_on_failure/test.py	15480
test_postgresql_protocol/test_kill_query.py	15477
test_backward_compatibility/test_cte_distributed.py	15420
test_attach_without_checksums/test.py	15395
test_s3_style_link/test.py	15314
test_storage_iceberg_with_spark/test_iceberg_history_summary.py	15312
test_build_sets_from_multiple_threads/test.py	15297
test_materialized_view_restart_server/test.py	15242
test_merge_tree_prewarm_cache/test.py	15231
test_insert_query_profile_events/test.py	15214
test_prometheus_protocols/test_query_api.py	15208
test_projection_rebuild_with_required_columns/test.py	15177
test_settings_constraints_config_profiles/test.py	15154
test_http_connection_drain_before_reuse/test.py	15127
test_shard_names/test.py	15120
test_user_zero_database_access/test_user_zero_database_access.py	15093
test_dictionary_custom_settings/test.py	15036
test_storage_iceberg_with_spark/test_pruning_nullable_bug.py	15036
test_grpc_protocol_ssl/test.py	14994
test_dot_in_user_name/test.py	14832
test_dotnet_client/test.py	14692
test_jdbc_bridge_hang/test.py	14599
test_jemalloc_merge_tree_arenas/test.py	14596
test_merge_tree_load_marks/test.py	14540
test_config_xml_full/test.py	14533
test_arrowflight_interface/test_ticket_expiration.py	14521
test_storage_iceberg_no_spark/test_writes_with_snappy_compression_metadata.py	14505
test_tcp_handler_connection_limits/test.py	14502
test_drop_data/test.py	14491
test_zookeeper_info/test.py	14449
test_filesystem/test.py	14446
test_distributed_config/test.py	14440
test_concurrent_queries_for_all_users_restriction/test.py	14436
test_ttl_to_disk_wrapped_by_cache/test.py	14412
test_backward_compatibility/test_short_strings_aggregation.py	14343
test_cache_s3_object_truncation/test.py	14326
test_storage_iceberg_with_spark/test_partition_by.py	14326
test_backward_compatibility/test_normalized_count_comparison.py	14272
test_disabled_access_control_improvements/test_impersonate_user.py	14132
test_s3_with_https/test.py	14093
test_tlsv1_3/test.py	14081
test_storage_azure_blob_storage/test_check_after_upload.py	14070
test_backward_compatibility/test_select_aggregate_alias_column.py	14062
test_config_xml_yaml_mix/test.py	14061
test_storage_iceberg_with_spark/test_writes_field_partitioning.py	13964
test_paimon_spark_smoke/test.py	13937
test_ssh_keys_authentication/test.py	13921
test_http_header_limits/test.py	13918
test_user_grants_from_config/test.py	13868
test_enabling_access_management/test.py	13816
test_accept_invalid_certificate/test.py	13755
test_users_config_include_from_reload/test.py	13752
test_s3_imds/test_session_token.py	13742
test_keeper_dynamic_log_level/test.py	13721
test_allow_plaintext_and_no_password/test.py	13717
test_system_reload_async_metrics/test.py	13716
test_database_iceberg_seaweedfs_catalog/test.py	13627
test_distributed_plan_cancel/test.py	13600
test_spark_session_recovery/test.py	13591
test_zookeeper_info_number_overflow/test.py	13591
test_skip_local_missing_table/test.py	13584
test_config_yaml_full/test.py	13578
test_user_query_log_disabled/test.py	13572
test_s3_storage_class_multipart/test.py	13551
test_prometheus_protocols/test_series_api.py	13518
test_storage_url_http_headers/test.py	13509
test_config_yaml_main/test.py	13444
test_text_log_level/test.py	13444
test_config_yaml_merge_keys/test.py	13328
test_keeper_availability_zone_quorum_reads/test.py	13319
test_keeper_four_word_command/test_allow_list.py	13280
test_executable_udf_driver_config_reload/test.py	13276
test_custom_settings/test.py	13257
test_merge_tree_settings_constraints/test.py	13252
test_naive_bayes_xml_dictionary/test.py	13185
test_concurrent_queries_for_user_restriction/test.py	13166
test_storage_iceberg_no_spark/test_writes_with_compression_metadata.py	13162
test_http_dictionary_named_collection/test.py	13108
test_storage_url_last_modified/test.py	13082
test_parquet_page_index/test.py	12883
test_geoparquet/test.py	12845
test_dictionaries_null_value/test.py	12808
test_keeper_path_acl/test.py	12807
test_interserver_tables_status_auth/test.py	12805
test_aggregating_in_order_transform_kill_query/test.py	12794
test_unknown_column_dist_table_with_alias/test.py	12708
test_freeze_table/test.py	12684
test_keeper_invalid_digest/test.py	12588
test_system_grants_url_regexp/test.py	12574
test_inherit_multiple_profiles/test.py	12515
test_parallel_replicas_skip_shards/test.py	12485
test_system_users_predicate_pushdown/test.py	12401
test_structured_logging_json/test.py	12381
test_reload_query_masking_rules/test.py	12379
test_config_xml_main/test.py	12354
test_buffer_profile/test.py	12316
test_allow_implicit_no_password/test.py	12306
test_disk_name_virtual_column/test.py	12304
test_config_hide_in_preprocessed/test.py	12298
test_keeper_profiler/test.py	12276
test_remap_executable/test.py	12211
test_user_query_log_distributed_backend/test.py	12146
test_keeper_secure_client/test.py	12122
test_keeper_nuraft_streaming/test.py	12087
test_config_decryption/test.py	12081
test_s3_non_deterministic_partition_by/test.py	12066
test_keeper_and_access_storage/test.py	12060
test_backward_compatibility/test_insert_profile_events.py	12042
test_keeper_compression/test_with_compression.py	12041
test_keeper_http_storage_control/test.py	12018
test_memory_thread_stacks_metric/test.py	12009
test_prometheus_protocols/test_http_port.py	12008
test_custom_dashboards/test.py	11994
test_keeper_compression/test_without_compression.py	11980
test_compatibility_readonly_constrained_setting/test.py	11902
test_dictionaries_with_invalid_structure/test.py	11901
test_native_incorrect_data_deserialization/test.py	11826
test_thread_pool_queue_size/test.py	11820
test_disk_types/test.py	11740
test_storage_iceberg_with_spark/test_single_iceberg_file.py	11738
test_storage_iceberg_with_spark/test_writes_multiple_threads.py	11738
test_remote_function_view/test.py	11694
test_keeper_memory_soft_limit_ratio/test.py	11645
test_s3_redirect_remote_host_filter/test.py	11630
test_passing_max_partitions_to_read_remotely/test.py	11627
test_s3_storage_class/test.py	11617
test_remote_prewhere/test.py	11566
test_async_metrics_in_cgroup/test.py	11472
test_backup_s3_storage_class/test.py	11434
test_range_hashed_dictionary_types/test.py	11348
test_storage_iceberg_with_spark/test_minmax_pruning_for_arrays_and_maps_subfields_disabled.py	11324
test_internal_queries_not_counted/test.py	11296
test_settings_randomization/test.py	11285
test_server_keep_alive/test.py	11174
test_endpoint_macro_substitution/test.py	11160
test_interserver_marker_requires_cluster_secret/test.py	11136
test_server_initialization/test.py	11119
test_core_dump_size_limit/test.py	11111
test_play_image_preview/test.py	11084
test_profile_settings_and_constraints_order/test.py	10970
test_arrowflight_session_log/test.py	10955
test_relative_filepath/test.py	10907
test_storage_url_with_proxy/test.py	10898
test_global_overcommit_tracker/test.py	10894
test_overcommit_tracker/test.py	10805
test_composable_protocol_without_global_ssl/test.py	10798
test_cancel_freeze/test.py	10789
test_send_crash_reports/test.py	10749
test_arrowflight_interface/test_prepared_statement_limit.py	10731
test_play_chart_helpers/test.py	10724
test_storage_iceberg_with_spark/test_manifest_read_performance.py	10719
test_backward_compatibility/test_old_client_with_replicated_columns.py	10669
test_keeper_memory_soft_limit/test.py	10657
test_storage_iceberg_with_spark/test_metadata_file_path_security.py	10558
test_trace_collector_serverwide/test.py	10540
test_filesystem_cache/test_size_limit_metric.py	10366
test_ssh/test_options_propagation_enabled.py	10358
test_merge_tree_check_part_with_cache/test.py	10352
test_format_cannot_allocate_thread/test.py	10342
test_tcp_query_body_oversized_read/test.py	10335
test_delayed_remote_source/test.py	10325
test_jemalloc_global_profiler/test.py	10315
test_async_metrics_overload_warning/test.py	10293
test_trace_log_memory_context/test.py	10279
test_tcp_handler_interserver_listen_host/test_case.py	10271
test_memory_profiler_min_max_borders/test.py	10269
test_dirty_pages_force_purge/test.py	10262
test_memory_limit/test.py	10252
test_logs_level/test.py	10243
test_jemalloc_profiler_sampling_rate/test.py	10242
test_render_log_file_name_templates/test.py	10188
test_host_regexp_hosts_file_resolution/test.py	10166
test_tcp_handler_http_responses/test_case.py	10152
test_webterminal_startup/test.py	10148
test_system_reload_async_metrics/test_async_metrics_invalid_settings.py	9982
test_http_auth_config_credentials/test.py	9951
test_cgroup_metrics/test.py	9935
test_storage_iceberg_no_spark/test_iceberg_history_operation_summary.py	9922
test_arrowflight_interface/test_prepared_statement_malformed_params.py	9909
test_filesystem_cache_uninitialized/test.py	9874
test_storage_iceberg_with_spark/test_relevant_iceberg_schema_chosen.py	9812
test_storage_iceberg_with_spark/test_delete_manifest_decode_concurrency.py	9807
test_userspace_page_cache/test_incorrect_limits.py	9745
test_custom_http_handlers_per_protocol/test.py	9724
test_validate_threadpool_writer_pool_size/test.py	9578
test_storage_iceberg_no_spark/test_writes_nullable_bugs2.py	9466
test_storage_iceberg_no_spark/test_time_travel_bug_fix_validation.py	9444
test_config_corresponding_root/test.py	9317
test_storage_iceberg_with_spark/test_compressed_metadata.py	9154
test_storage_iceberg_no_spark/test_iceberg_history_missing_optional_summary_metrics.py	9147
test_keeper_watch_profile_events/test.py	9037
test_database_disk/test.py	8460
test_storage_iceberg_with_spark/test_dates.py	8142
test_storage_iceberg_with_spark/test_writes_drop_table.py	8114
test_keeper_http_control_readiness/test.py	8104
test_keeper_https_control_standalone_cluster/test.py	7268
test_storage_iceberg_no_spark/test_writes_create_table_bugs.py	7094
test_storage_iceberg_no_spark/test_graceful_error_not_configured_iceberg_metadata_log.py	6972
test_storage_iceberg_with_spark/test_writes_create_version_hint.py	6830
test_storage_iceberg_with_spark/test_cluster_table_function_with_partition_pruning.py	6680
test_keeper_http_control_cli/test.py	6140
test_keeper_request_total_with_subrequests/test.py	5859
test_keeper_http_jemalloc/test.py	5705
test_keeper_https_control_cli/test.py	5637
test_kafka_bad_messages/test_delete_topic_helper.py	5005
test_storage_iceberg_with_spark/test_multiple_partitions_on_one_column.py	4981
test_storage_iceberg_with_spark/test_writes_different_path_format_error.py	4698
test_cgroup_limit/test.py	4355
test_disks_app_interactive/test.py	2392
test_storage_iceberg_with_spark/test_local_table_safety.py	1464
test_jemalloc_percpu_arena/test.py	1272
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
