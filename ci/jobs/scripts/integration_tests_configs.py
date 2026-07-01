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


# Long-running test suites, used by get_optimal_test_batch to balance shards by duration.
# Regenerate periodically (it drifts as tests change) with the query below on play.clickhouse.com.
# The 60000ms floor keeps the table broad: suites without an entry get weight 0 and are only
# round-robin distributed, so the more wall-clock mass the table covers, the better the packer
# balances. The path filter drops functional-test rows that share the integration check name.
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
HAVING dur > 60000
ORDER BY dur DESC, test_suite ASC;
"""

RAW_TEST_DURATIONS = """
test_storage_s3_queue/test_6.py	1599033
test_storage_delta/test.py	1399857
test_scheduler_cpu_preemptive/test.py	1395690
test_storage_kafka/test_batch_fast.py	1359709
test_storage_s3_queue/test_5.py	1302132
test_database_replicated_settings/test.py	1155494
test_max_bytes_ratio_before_external_order_group_by_for_server/test.py	1118568
test_storage_rabbitmq/test.py	1020871
test_storage_s3/test.py	983356
test_multiple_disks/test.py	946190
test_replicated_database/test.py	881567
test_ttl_replicated/test.py	873453
test_storage_azure_blob_storage/test.py	872539
test_backup_restore_new/test.py	854280
test_dictionaries_all_layouts_separate_sources/test_mongo.py	849456
test_dictionaries_redis/test.py	792488
test_backup_restore_s3/test.py	790779
test_refreshable_mat_view/test.py	781240
test_named_collections/test.py	752934
test_distributed_load_balancing/test.py	625702
test_ttl_move/test.py	599481
test_storage_s3_queue/test_migration.py	568680
test_dictionaries_all_layouts_separate_sources/test_clickhouse_remote.py	549938
test_dictionaries_all_layouts_separate_sources/test_clickhouse_local.py	547214
test_dictionaries_all_layouts_separate_sources/test_mysql.py	542947
test_dictionaries_all_layouts_separate_sources/test_http.py	533950
test_dictionaries_all_layouts_separate_sources/test_https.py	532808
test_async_load_databases/test.py	529911
test_backup_restore_on_cluster/test_concurrency.py	513010
test_storage_s3_queue/test_2.py	509489
test_storage_s3_queue/test_0.py	504711
test_storage_iceberg_with_spark/test_minmax_pruning.py	480305
test_mask_sensitive_info/test.py	467405
test_distributed_ddl/test.py	449795
test_restore_db_replica/test.py	434486
test_log_query_probability/test.py	431744
test_merge_tree_s3/test.py	402331
test_parallel_replicas_insert_select/test.py	393947
test_database_iceberg/test.py	389068
test_concurrent_ttl_merges/test.py	386290
test_storage_iceberg_with_spark/test_cluster_table_function.py	385332
test_refreshable_mv/test.py	379758
test_database_delta/test.py	378261
test_storage_s3_queue/test_1.py	368332
test_checking_s3_blobs_paranoid/test.py	362656
test_executable_table_function/test.py	361844
test_named_collections_encrypted2/test.py	355884
test_backup_restore_on_cluster/test.py	355552
test_refreshable_mat_view_replicated/test.py	355015
test_lost_part_during_startup/test.py	354182
test_scheduler/test.py	351404
test_ytsaurus/test_tables.py	349422
test_mysql_database_engine/test.py	348017
test_storage_iceberg_schema_evolution/test_evolved_schema_simple.py	343043
test_storage_kafka/test_batch_slow_4.py	338052
test_kafka_bad_messages/test.py	326491
test_storage_kafka/test_batch_slow_2.py	323002
test_storage_iceberg_with_spark/test_expire_snapshots.py	320142
test_dictionaries_dependency/test.py	318634
test_drop_is_lock_free/test.py	307856
test_postgresql_replica_database_engine/test_3.py	306612
test_dns_cache/test.py	302325
test_mysql57_database_engine/test.py	297793
test_storage_kafka/test_batch_slow_1.py	293504
test_dictionaries_ddl/test.py	293084
test_storage_kafka/test_batch_slow_5.py	292760
test_storage_postgresql/test.py	290890
test_mysql_protocol/test.py	290639
test_row_policy/test.py	289791
test_filesystem_split_cache/test.py	284193
test_storage_iceberg_with_spark/test_partition_pruning.py	278505
test_postgresql_replica_database_engine/test_2.py	277740
test_throttling/test.py	273055
test_crash_log/test.py	272533
test_distributed_directory_monitor_split_batch_on_failure/test.py	270725
test_migration_deduplication_hash/test.py	266196
test_storage_iceberg_with_spark/test_system_iceberg_metadata.py	265940
test_storage_kafka/test_compression_codec.py	260916
test_storage_iceberg_with_trino/test.py	256957
test_keeper_ttl_nodes/test.py	254893
test_dictionaries_all_layouts_separate_sources/test_file.py	254254
test_postpone_failed_tasks/test.py	248497
test_backward_compatibility/test_aggregate_function_state.py	246977
test_cleanup_dir_after_bad_zk_conn/test.py	243628
test_parallel_replicas_invisible_parts/test.py	243355
test_database_glue/test.py	239939
test_storage_kafka/test_batch_slow_6.py	239672
test_hedged_requests/test.py	236021
test_s3_plain_rewritable/test.py	235996
test_postgresql_replica_database_engine/test_1.py	235407
test_storage_s3_queue/test_3.py	232739
test_storage_iceberg_schema_evolution/test_tuple_evolved_simple.py	225756
test_postgresql_replica_database_engine/test_0.py	219088
test_storage_s3_queue/test_4.py	217434
test_storage_kafka/test_batch_slow_0.py	212284
test_insert_distributed_async_send/test.py	210229
test_storage_s3_queue/test_flush.py	201588
test_implicit_index_upgrade/test.py	200633
test_s3_aws_sdk_has_slightly_unreliable_behaviour/test.py	199112
test_backup_restore_on_cluster/test_cancel_backup.py	198517
test_polymorphic_parts/test.py	198478
test_parallel_replicas_over_distributed/test.py	195894
test_prometheus_protocols/test_evaluation.py	193806
test_storage_iceberg_with_spark/test_position_deletes.py	193514
test_broken_projections/test.py	191680
test_scheduler_memory/test.py	188273
test_jbod_balancer/test.py	185475
test_storage_iceberg_with_spark/test_writes.py	184416
test_storage_iceberg_schema_evolution/test_array_evolved_nested.py	183175
test_dictionaries_all_layouts_separate_sources/test_executable_hashed.py	181272
test_storage_mongodb/test.py	181180
test_filesystem_cache/test.py	180890
test_lost_part/test.py	178618
test_distributed_index_analysis/test.py	178540
test_backward_compatibility/test_convert_ordinary.py	176898
test_http_failover/test.py	174582
test_grant_and_revoke/test_with_table_engine_grant.py	174540
test_dictionaries_all_layouts_separate_sources/test_mongo_uri.py	174468
test_storage_kerberized_kafka/test.py	174242
test_ytsaurus/test_dictionaries.py	174049
test_disk_over_web_server/test.py	173277
test_backup_restore_new/test_cancel_backup.py	173072
test_storage_iceberg_no_spark/test_writes_statistics_by_minmax_pruning.py	172610
test_stop_insert_when_disk_close_to_full/test.py	172483
test_executable_udf_async_metrics/test.py	168403
test_storage_kafka/test_kafka_zone_awareness.py	166628
test_distributed_frozen_replica/test.py	165102
test_plain_rewritable_backward_compatibility/test.py	163030
test_system_logs/test_system_logs.py	161333
test_library_bridge/test.py	160646
test_merge_tree_azure_blob_storage/test.py	160373
test_http_handlers_config/test.py	160132
test_keeper_two_nodes_cluster/test.py	157693
test_keeper_internal_secure/test.py	155634
test_replicated_mutations/test.py	155222
test_keeper_snapshot_chunked_transfer/test.py	155016
test_manipulate_statistics/test.py	152254
test_s3_plain_rewritable_rotate_tables/test.py	152236
test_cluster_discovery/test.py	151966
test_dictionaries_all_layouts_separate_sources/test_executable_cache.py	150601
test_dictionaries_update_and_reload/test.py	150342
test_storage_iceberg_with_spark/test_writes_mutate_delete.py	149634
test_drop_database_replica/test.py	149317
test_parallel_replicas_custom_key_failover/test.py	144980
test_encrypted_disk/test.py	144464
test_keeper_zookeeper_converter/test.py	144080
test_storage_iceberg_concurrent/test_concurrent_reads.py	143770
test_statistics_cache/test.py	141470
test_asynchronous_metrics_pk_bytes_fields/test.py	141466
test_replicated_fetches_bandwidth/test.py	140385
test_keeper_map/test.py	140005
test_system_clusters_actual_information/test.py	138508
test_transactions/test.py	138153
test_storage_iceberg_with_spark/test_remove_orphan_files.py	137180
test_kafka_bad_messages/test_mv_target_missing.py	133855
test_role/test.py	133751
test_storage_mysql/test.py	131506
test_inserts_with_keeper_retries/test.py	130547
test_system_logs_recreate/test.py	127910
test_ddl_worker_replicas/test.py	125643
test_attach_without_fetching/test.py	124387
test_refreshable_mv_skip_old_temp_table_ddls/test.py	123741
test_quota/test.py	119557
test_parallel_replicas_custom_key_load_balancing/test.py	117379
test_recompression_ttl/test.py	117330
test_disk_access_storage/test.py	116955
test_storage_iceberg_schema_evolution/test_tuple_evolved_nested.py	116475
test_distributed_ddl_parallel/test.py	115938
test_MemoryTracking/test.py	115759
test_prometheus_protocols/test_different_table_engines.py	115668
test_scheduler_query/test.py	114546
test_storage_iceberg_disks/test.py	114149
test_storage_kafka/test_produce_http_interface.py	113806
test_version_update_after_mutation/test.py	113235
test_named_collections_encrypted2/test_integr.py	112798
test_storage_iceberg_with_spark/test_writes_create_partitioned_table.py	112458
test_quorum_inserts/test.py	112194
test_mutations_with_merge_tree/test.py	109381
test_text_index_upgrade/test.py	109263
test_restore_replica/test.py	109058
test_replicated_user_defined_functions/test.py	108789
test_disk_configuration/test.py	108423
test_temporary_data_in_cache/test.py	108120
test_partition/test.py	107912
test_recovery_replica/test.py	107908
test_system_merges/test.py	107302
test_merges_memory_limit/test.py	104550
test_replicated_users/test.py	102254
test_database_disk_setting/test.py	101840
test_scheduler_cpu/test.py	101271
test_table_db_num_limit/test.py	100353
test_reloading_storage_configuration/test.py	100281
test_executable_dictionary/test.py	100002
test_backward_compatibility/test_functions.py	99417
test_globs_in_filepath/test.py	98502
test_select_access_rights/test_main.py	97688
test_zookeeper_config/test_secure.py	96929
test_postgresql_database_engine/test.py	95671
test_alter_moving_garbage/test.py	95614
test_rename_column/test.py	95295
test_modify_engine_on_restart/test_ordinary.py	93256
test_backup_restore_keeper_map/test.py	93076
test_keeper_reconfig_replace_leader_in_one_command/test.py	92097
test_database_backup/test.py	92013
test_s3_cluster/test.py	91940
test_storage_iceberg_with_spark/test_writes_mutate_update.py	90469
test_executable_user_defined_function/test.py	90285
test_merge_tree_load_parts/test.py	90016
test_settings_profile/test.py	89979
test_rocksdb_options/test.py	89438
test_server_reload/test.py	89022
test_keeper_incorrect_config/test.py	88389
test_keeper_remove_rejoin_leader/test.py	88066
test_distributed_inter_server_secret/test.py	88011
test_restore_replica_metadata_version/test.py	87867
test_cluster_discovery/test_auxiliary_keeper.py	87446
test_keeper_password/test.py	87308
test_azure_blob_storage_plain_rewritable/test.py	87088
test_format_schema_source/test.py	86927
test_cluster_discovery/test_dynamic_clusters.py	86579
test_storage_s3_queue/test_parallel_inserts.py	86473
test_system_metrics/test.py	84624
test_system_start_stop_listen/test.py	84597
test_distributed_ddl/test_replicated_alter.py	83979
test_keeper_three_nodes_two_alive/test.py	83611
test_storage_iceberg_with_spark/test_schema_inference.py	83552
test_s3_table_function_with_https_proxy/test.py	82997
test_keeper_opentelemetry_tracing/test.py	82949
test_storage_iceberg_with_spark/test_query_condition_cache.py	82940
test_s3_table_function_with_http_proxy/test.py	82849
test_zookeeper_send_window_broken_promise/test.py	82845
test_storage_delta_disks/test.py	82354
test_backup_restore_on_cluster_with_checksum_data_file_name/test.py	82226
test_storage_iceberg_with_spark/test_schema_evolution_with_time_travel.py	82204
test_restore_external_engines/test.py	82120
test_storage_hudi/test.py	81993
test_executable_udf_profile_events/test.py	81458
test_s3_cluster_restart/test.py	81451
test_refreshable_mv_watch_fault/test.py	81252
test_backup_restore_azure_blob_storage/test.py	80688
test_ddl_create_then_alter_offline_replica/test.py	80309
test_consistant_parts_after_move_partition/test.py	80154
test_storage_iceberg_with_spark/test_writes_create_table.py	79964
test_rmv_access_denied_on_rename_race/test.py	79682
test_jbod_ha/test.py	79681
test_remote_blobs_naming/test_backward_compatibility.py	79599
test_replicated_merge_tree_compatibility/test.py	79514
test_store_cleanup/test.py	79320
test_paimon_incremental_read/test.py	79095
test_storage_iceberg_with_spark/test_metadata_file_format_with_uuid.py	78918
test_system_flush_logs/test.py	78482
test_dictionaries_postgresql/test.py	78276
test_storage_iceberg_with_spark/test_metadata_file_selection.py	77869
test_clickhouse_server_wait_server_pool/test.py	77474
test_hedged_requests_parallel/test.py	76542
test_keeper_ttl_nodes/test_disabled.py	76310
test_dictionaries_dependency_xml/test.py	75950
test_check_table/test.py	75768
test_storage_kafka/test_avro_schema_registry.py	74728
test_keeper_force_recovery/test.py	74586
test_keeper_back_to_back/test.py	73390
test_file_schema_inference_cache/test.py	73190
test_mark_cache_profile_events/test.py	72942
test_storage_kafka_sasl/test.py	72696
test_catboost_evaluate/test.py	72539
test_multi_access_storage_role_management/test.py	72168
test_keeper_disks/test.py	72149
test_replicated_table_attach/test.py	72006
test_parallel_replicas_custom_key/test.py	70877
test_keeper_persistent_watches/test.py	70768
test_backward_compatibility/test_aggregate_function_state_contingency_functions.py	70382
test_keeper_nodes_remove/test.py	70137
test_storage_iceberg_schema_evolution/test_array_evolved_with_struct.py	69870
test_server_overload/test.py	69662
test_default_compression_codec/test.py	68093
test_random_inserts/test.py	68052
test_merge_tree_s3_failover/test.py	67947
test_insert_into_distributed/test.py	67817
test_replicated_database_recover_digest_mismatch/test.py	66731
test_storage_iceberg_schema_evolution/test_evolved_schema_complex.py	66586
test_distributed_plan_replicated_merge_tree/test.py	66494
test_replicated_database_cluster_groups/test.py	66414
test_sharding_key_from_default_column/test.py	65939
test_ai_functions/test.py	65660
test_storage_kafka/test_poll_timeout_after_assignment.py	65441
test_https_replication/test.py	64758
test_async_insert_memory/test.py	64222
test_backup_restore_on_cluster/test_disallow_concurrency.py	64167
test_keeper_4lw_reconfiguration/test.py	63954
test_keeper_reconfig_remove_many/test.py	63818
test_dictionaries_mysql/test.py	63698
test_backward_compatibility/test_aggregate_function_state_tuple_return_type.py	63486
test_modify_engine_on_restart/test.py	63368
test_storage_iceberg_with_spark/test_delete_files.py	63290
test_named_collections_if_exists_on_cluster/test.py	63105
test_lightweight_updates/test.py	63072
test_storage_iceberg_with_spark/test_iceberg_snapshot_reads.py	62960
test_storage_iceberg_with_spark/test_explicit_metadata_file.py	62838
test_system_detached_tables/test.py	62300
test_backward_compatibility/test_vertical_merges_from_compact_parts.py	62262
test_dictionaries_replace/test.py	60412
test_replication_credentials/test.py	60149
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
    # parallel_skip_prefixes sanity check
    for test_config in TEST_CONFIGS:
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
