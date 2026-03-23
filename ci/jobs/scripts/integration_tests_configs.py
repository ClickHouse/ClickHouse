import dataclasses


@dataclasses.dataclass
class TC:
    prefix: str
    is_sequential: bool
    comment: str


TEST_CONFIGS = [
    # TC("test_atomic_drop_table/", True, "no idea why i'm sequential"),
    # TC("test_attach_without_fetching/", True, "no idea why i'm sequential"),
    # TC(
    #     "test_cleanup_dir_after_bad_zk_conn/",
    #     True,
    #     "no idea why i'm sequential",
    # ),
    # TC(
    #     "test_consistent_parts_after_clone_replica/",
    #     True,
    #     "no idea why i'm sequential",
    # ),
    # TC("test_cross_replication/", True, "no idea why i'm sequential"),
    # TC("test_ddl_worker_non_leader/", True, "no idea why i'm sequential"),
    # TC("test_delayed_replica_failover/", True, "no idea why i'm sequential"),
    # TC("test_disabled_mysql_server/", True, "no idea why i'm sequential"),
    # 1
    # TC("test_system_metrics/", True, "no idea why i'm sequential"),
    # TC("test_ttl_move/", True, "no idea why i'm sequential"),
    # TC(
    #     "test_zookeeper_config_load_balancing/",
    #     True,
    #     "no idea why i'm sequential",
    # ),
    # TC("test_zookeeper_fallback_session/", True, "no idea why i'm sequential"),
    # TC("test_keeper_map/", True, "no idea why i'm sequential"),
    # TC("test_keeper_multinode_simple/", True, "no idea why i'm sequential"),
    # TC("test_keeper_two_nodes_cluster/", True, "no idea why i'm sequential"),
    # TC("test_mysql_database_engine/", True, "no idea why i'm sequential"),
    # TC("test_replace_partition/", True, "no idea why i'm sequential"),
    # TC("test_replicated_fetches_timeouts/", True, "no idea why i'm sequential"),
    # TC(
    #     "test_replicated_merge_tree_wait_on_shutdown/",
    #     True,
    #     "no idea why i'm sequential",
    # ),
    # 2
    # TC("test_distributed_respect_user_timeouts/", True, "no idea why i'm sequential"),
    # TC("test_limited_replicated_fetches/", True, "no idea why i'm sequential"),
    # TC("test_parts_delete_zookeeper/", True, "no idea why i'm sequential"),
    # TC("test_insert_into_distributed/", True, "no idea why i'm sequential"),
    # TC(
    #     "test_insert_into_distributed_through_materialized_view/",
    #     True,
    #     "no idea why i'm sequential",
    # ),
    # TC(
    #     "test_postgresql_replica_database_engine/",
    #     True,
    #     "no idea why i'm sequential",
    # ),
    # 3 not a problem
    # TC("test_quorum_inserts_parallel/", True, "no idea why i'm sequential"),
    # TC("test_storage_s3/", True, "no idea why i'm sequential"),
    # Sequential
    TC("test_crash_log/", True, "no idea why i'm sequential"),
    TC(
        "test_dictionary_allow_read_expired_keys/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_dns_cache/", True, "no idea why i'm sequential"),
    TC("test_global_overcommit_tracker/", True, "no idea why i'm sequential"),
    TC("test_grpc_protocol/", True, "no idea why i'm sequential"),
    TC("test_host_regexp_multiple_ptr_records/", True, "no idea why i'm sequential"),
    TC("test_http_failover/", True, "no idea why i'm sequential"),
    TC("test_https_replication/", True, "no idea why i'm sequential"),
    TC("test_merge_tree_s3/", True, "no idea why i'm sequential"),
    TC(
        "test_profile_max_sessions_for_user/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_random_inserts/", True, "no idea why i'm sequential"),
    TC("test_replicated_database/", True, "no idea why i'm sequential"),
    TC("test_server_overload/", True, "no idea why i'm sequential"),
    TC("test_server_reload/", True, "no idea why i'm sequential"),
    TC("test_storage_kafka/", True, "no idea why i'm sequential"),
    TC("test_storage_kerberized_kafka/", True, "no idea why i'm sequential"),
    TC("test_storage_s3_queue/", True, "no idea why i'm sequential"),
    TC("test_system_flush_logs/", True, "no idea why i'm sequential"),
    TC("test_system_logs/", True, "no idea why i'm sequential"),
    TC("test_user_ip_restrictions/", True, "no idea why i'm sequential"),
    TC(
        "test_backup_restore_on_cluster/test_concurrency.py",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_database_delta/test.py", True, "no idea why i'm sequential"),
    TC("test_keeper_ipv4_fallback/test.py", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_no_spark/", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_with_spark/", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_with_spark_cache/", True, "no idea why i'm sequential"),
    TC(
        "test_storage_iceberg_schema_evolution/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_disks_app_interactive/", True, "no idea why i'm sequential"),
    TC("test_storage_delta_disks/", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_disks/", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_concurrent/", True, "no idea why i'm sequential"),
]

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

# collected by
# SELECT
#     splitByString('::', test_name)[1] AS test_file,
#     SUM(test_duration_ms) AS dur
# FROM checks
# WHERE 1
#   AND check_name LIKE 'Integration tests (amd_binary, %/5)'
#   AND commit_sha = '1cc16f7ff0babee1e8c450756d7b1b18b18e1b5e'
# GROUP BY test_file
# HAVING test_file != ''
# ORDER BY dur desc
RAW_TEST_DURATIONS = """
test_storage_s3_queue/test_5.py	918869
test_storage_kafka/test_batch_fast.py	861725
test_storage_delta/test.py	776078
test_storage_nats/test_nats_jet_stream.py	701045
test_ttl_move/test.py	666451
test_storage_s3/test.py	561548
test_backup_restore_on_cluster/test_concurrency.py	417547
test_executable_table_function/test.py	364426
test_replicated_database/test.py	337507
test_storage_kafka/test_batch_slow_1.py	333467
test_lost_part_during_startup/test.py	332887
test_restore_db_replica/test.py	321357
test_distributed_respect_user_timeouts/test.py	319713
test_s3_aws_sdk_has_slightly_unreliable_behaviour/test.py	300345
test_storage_iceberg_with_spark/test_cluster_table_function.py	293985
test_max_bytes_ratio_before_external_order_group_by_for_server/test.py	281407
test_database_delta/test.py	276025
test_storage_azure_blob_storage/test.py	272009
test_refreshable_mat_view_replicated/test.py	262379
test_multiple_disks/test.py	255071
test_kafka_bad_messages/test.py	251238
test_database_replicated_settings/test.py	249358
test_storage_kafka/test_batch_slow_2.py	244858
test_checking_s3_blobs_paranoid/test.py	235332
test_storage_s3_queue/test_1.py	233167
test_backup_restore_s3/test.py	230946
test_throttling/test.py	230781
test_merge_tree_s3/test.py	220731
test_refreshable_mv/test.py	218699
test_backup_restore_new/test.py	213669
test_http_handlers_config/test.py	205439
test_storage_s3_queue/test_2.py	203033
test_storage_nats/test_nats_core.py	186159
test_storage_kafka/test_batch_slow_5.py	183438
test_drop_is_lock_free/test.py	178856
test_concurrent_ttl_merges/test.py	175646
test_storage_kafka/test_batch_slow_4.py	175219
test_dns_cache/test.py	173257
test_storage_kafka/test_compression_codec.py	172654
test_keeper_two_nodes_cluster/test.py	170523
test_named_collections/test.py	167668
test_storage_kafka/test_batch_slow_6.py	163169
test_refreshable_mat_view/test.py	158824
test_ytsaurus/test_tables.py	158155
test_inserts_with_keeper_retries/test.py	157484
test_storage_s3_queue/test_0.py	152699
test_crash_log/test.py	150340
test_hedged_requests/test.py	148580
test_async_load_databases/test.py	147986
test_statistics_cache/test.py	146352
test_backup_restore_new/test_cancel_backup.py	144732
test_backward_compatibility/test_aggregate_function_state.py	143799
test_lost_part/test.py	142405
test_http_failover/test.py	142064
test_storage_iceberg_schema_evolution/test_evolved_schema_simple.py	140896
test_storage_kerberized_kafka/test.py	140642
test_refreshable_mv_skip_old_temp_table_ddls/test.py	140440
test_system_logs/test_system_logs.py	130706
test_postgresql_database_engine/test.py	128499
test_keeper_zookeeper_converter/test.py	122893
test_system_clusters_actual_information/test.py	119814
test_distributed_ddl/test.py	118591
test_replicated_mutations/test.py	117696
test_plain_rewritable_backward_compatibility/test.py	117391
test_dictionaries_all_layouts_separate_sources/test_mongo.py	115720
test_distributed_load_balancing/test.py	115337
test_dictionaries_dependency/test.py	113853
test_library_bridge/test.py	113850
test_mask_sensitive_info/test.py	111736
test_backward_compatibility/test_functions.py	111614
test_rename_column/test.py	109414
test_backup_restore_on_cluster/test_cancel_backup.py	106758
test_scheduler/test.py	104465
test_postpone_failed_tasks/test.py	104415
test_backward_compatibility/test_convert_ordinary.py	103586
test_storage_iceberg_concurrent/test_concurrent_reads.py	100653
test_storage_iceberg_with_spark/test_position_deletes.py	99719
test_storage_s3_queue/test_3.py	99349
test_system_merges/test.py	93935
test_storage_iceberg_schema_evolution/test_array_evolved_nested.py	93912
test_ttl_replicated/test.py	91875
test_odbc_interaction/test.py	85081
test_keeper_internal_secure/test.py	84841
test_keeper_auth/test.py	84510
test_restore_replica/test.py	84405
test_backup_restore_on_cluster/test.py	83594
test_storage_kafka/test_batch_slow_0.py	83573
test_postgresql_replica_database_engine/test_1.py	82480
test_mysql_protocol/test.py	82215
test_ytsaurus/test_dictionaries.py	80993
test_keeper_map/test.py	80241
test_keeper_three_nodes_two_alive/test.py	78309
test_memory_limit_observer/test.py	77702
test_modify_engine_on_restart/test_ordinary.py	76617
test_dictionaries_redis/test.py	75885
test_storage_s3_queue/test_4.py	75736
test_parallel_replicas_insert_select/test.py	75129
test_keeper_nodes_remove/test.py	73922
test_dictionaries_all_layouts_separate_sources/test_mysql.py	73040
test_dictionaries_all_layouts_separate_sources/test_clickhouse_local.py	73021
test_dictionaries_all_layouts_separate_sources/test_http.py	72908
test_dictionaries_all_layouts_separate_sources/test_https.py	72883
test_dictionaries_all_layouts_separate_sources/test_clickhouse_remote.py	72696
test_cleanup_dir_after_bad_zk_conn/test.py	71610
test_log_query_probability/test.py	71173
test_s3_plain_rewritable/test.py	71104
test_keeper_incorrect_config/test.py	70984
test_recompression_ttl/test.py	69025
test_config_decryption/test_wrong_settings_zk.py	68173
test_store_cleanup/test.py	67868
test_keeper_password/test.py	67806
test_dictionaries_update_and_reload/test.py	67427
test_storage_hudi/test.py	66678
test_scheduler_cpu_preemptive/test.py	66600
test_rocksdb_options/test.py	66514
test_replicated_fetches_bandwidth/test.py	65311
test_s3_plain_rewritable_rotate_tables/test.py	65089
test_mysql_database_engine/test.py	65077
test_keeper_force_recovery/test.py	64144
test_ddl_worker_replicas/test.py	63931
test_postgresql_replica_database_engine/test_3.py	63689
test_s3_table_function_with_http_proxy/test.py	63655
test_mysql57_database_engine/test.py	63603
test_mutations_hardlinks/test.py	63105
test_random_inserts/test.py	62847
test_s3_table_function_with_https_proxy/test.py	62781
test_postgresql_replica_database_engine/test_2.py	62477
test_transposed_metric_log/test.py	62387
test_cluster_discovery/test_auxiliary_keeper.py	60904
test_version_update_after_mutation/test.py	60671
test_keeper_disks/test.py	60149
test_distributed_ddl_parallel/test.py	59635
test_alter_moving_garbage/test.py	58797
test_storage_iceberg_with_spark/test_minmax_pruning.py	58731
test_storage_iceberg_with_spark/test_metadata_file_format_with_uuid.py	58396
test_storage_iceberg_with_spark/test_metadata_file_selection.py	57780
test_keeper_back_to_back/test.py	57723
test_storage_alias_replicated/test.py	57001
test_cluster_discovery/test_dynamic_clusters.py	56144
test_globs_in_filepath/test.py	53734
test_replicated_user_defined_functions/test.py	53678
test_broken_projections/test.py	53675
test_executable_dictionary/test.py	52662
test_disk_over_web_server/test.py	52659
test_parallel_replicas_invisible_parts/test.py	50646
test_keeper_mntr_pressure/test.py	48972
test_database_backup/test.py	48573
test_storage_mongodb/test.py	48244
test_s3_table_functions/test.py	48130
test_drop_database_replica/test.py	47202
test_quorum_inserts/test.py	46950
test_cluster_discovery/test.py	46408
test_role/test.py	45959
test_filesystem_cache/test.py	45761
test_postgresql_replica_database_engine/test_0.py	45355
test_scheduler_query/test.py	44161
test_hedged_requests_parallel/test.py	44060
test_memory_limit/test.py	43773
test_keeper_znode_time/test.py	43261
test_reloading_storage_configuration/test.py	43094
test_backup_restore_new/test_shutdown_wait_backup.py	42975
test_merge_tree_load_parts/test.py	42141
test_keeper_snapshot_small_distance/test.py	41895
test_config_decryption/test_wrong_settings.py	40552
test_storage_iceberg_with_spark/test_system_iceberg_metadata.py	40493
test_parallel_replicas_over_distributed/test.py	40284
test_system_logs_recreate/test.py	39442
test_jbod_ha/test.py	39314
test_system_metrics/test.py	38955
test_keeper_nodes_add/test.py	38817
test_keeper_force_recovery_single_node/test.py	38434
test_merge_tree_azure_blob_storage/test.py	38425
test_storage_iceberg_with_spark/test_writes_mutate_delete.py	38301
test_dictionaries_ddl/test.py	37020
test_backup_restore_keeper_map/test.py	36220
test_storage_postgresql/test.py	35857
test_userspace_page_cache/test.py	35808
test_distributed_directory_monitor_split_batch_on_failure/test.py	35761
test_limited_replicated_fetches/test.py	35514
test_dictionaries_all_layouts_separate_sources/test_file.py	34992
test_named_collections_if_exists_on_cluster/test.py	34710
test_read_only_table/test.py	34655
test_s3_cluster_restart/test.py	34481
test_MemoryTracking/test.py	34099
test_old_parts_finally_removed/test.py	33870
test_storage_iceberg_with_spark/test_partition_pruning.py	33425
test_async_metrics_in_cgroup/test.py	33284
test_keeper_nodes_move/test.py	32554
test_azure_blob_storage_plain_rewritable/test.py	32413
test_replicated_database_recover_digest_mismatch/test.py	32136
test_storage_iceberg_schema_evolution/test_tuple_evolved_nested.py	32047
test_replicated_database_cluster_groups/test.py	31768
test_acme_tls/test_multi_node.py	31687
test_https_s3_table_function_with_http_proxy_no_tunneling/test.py	31531
test_keeper_block_acl/test.py	31493
test_parallel_replicas_all_marks_read/test.py	31037
test_storage_iceberg_with_spark/test_explicit_metadata_file.py	30960
test_host_regexp_multiple_ptr_records/test.py	30886
test_dictionaries_postgresql/test.py	30440
test_dictionaries_dependency_xml/test.py	30348
test_executable_user_defined_function/test.py	30297
test_disk_configuration/test.py	30212
test_server_overload/test.py	30074
test_row_policy/test.py	29978
test_encrypted_disk/test.py	29772
test_insert_distributed_async_send/test.py	29763
test_replicated_table_attach/test.py	29554
test_remove_stale_moving_parts/test.py	28915
test_storage_iceberg_with_spark/test_schema_evolution_with_time_travel.py	28535
test_merge_tree_s3_failover/test.py	28506
test_concurrent_queries_restriction_by_query_kind/test.py	28491
test_polymorphic_parts/test.py	28450
test_named_collections_encrypted/test.py	28397
test_disk_access_storage/test.py	28250
test_naive_bayes_bad_models/test.py	28166
test_keeper_reconfig_replace_leader_in_one_command/test.py	27982
test_user_directories/test.py	27414
test_ddl_on_cluster_stop_waiting_for_offline_hosts/test.py	27384
test_jemalloc_global_profiler/test.py	27266
test_keeper_invalid_digest/test.py	27194
test_keeper_snapshots/test.py	26846
test_zookeeper_config/test_password.py	26656
test_keeper_max_append_byte_size/test.py	26346
test_jbod_balancer/test.py	26322
test_storage_kafka_sasl/test.py	25888
test_group_array_element_size/test.py	25754
test_catboost_evaluate/test.py	25753
test_storage_iceberg_with_spark/test_writes_create_partitioned_table.py	25615
test_replicated_users/test.py	25492
test_storage_iceberg_no_spark/test_writes_statistics_by_minmax_pruning.py	25202
test_keeper_s3_snapshot/test.py	25193
test_consistent_parts_after_clone_replica/test.py	24971
test_dictionaries_all_layouts_separate_sources/test_executable_hashed.py	24823
test_on_cluster_timeouts/test.py	24753
test_async_insert_memory/test.py	24717
test_atomic_drop_table/test.py	24340
test_modify_engine_on_restart/test.py	24309
test_storage_iceberg_with_spark/test_writes_mutate_update.py	24287
test_storage_iceberg_schema_evolution/test_evolved_schema_complex.py	23839
test_keeper_broken_logs/test.py	23661
test_keeper_feature_flags_config/test.py	23438
test_storage_kafka/test_produce_http_interface.py	23262
test_storage_mysql/test.py	23064
test_backup_restore_on_cluster/test_disallow_concurrency.py	23004
test_replicated_merge_tree_with_auxiliary_zookeepers/test.py	22683
test_storage_iceberg_schema_evolution/test_array_evolved_with_struct.py	22221
test_parallel_replicas_custom_key_failover/test.py	22000
test_undrop_query/test.py	21949
test_keeper_multinode_simple/test.py	21863
test_move_ttl_broken_compatibility/test.py	21696
test_storage_iceberg_with_spark/test_metadata_file_selection_from_version_hint.py	21672
test_dictionaries_all_layouts_separate_sources/test_mongo_uri.py	21586
test_database_iceberg/test.py	21462
test_restore_external_engines/test.py	21371
test_s3_storage_conf_proxy/test.py	21103
test_system_flush_logs/test.py	21055
test_system_detached_tables/test.py	21054
test_parallel_replicas_custom_key_load_balancing/test.py	20778
test_dictionaries_config_reload/test.py	20634
test_executable_user_defined_functions_config_reload/test.py	20583
test_delayed_replica_failover/test.py	20285
test_database_disk_setting/test.py	20008
test_tmp_policy/test.py	19954
test_merges_memory_limit/test.py	19614
test_user_valid_until/test.py	19594
test_storage_iceberg_with_spark/test_schema_inference.py	19590
test_dictionaries_all_layouts_separate_sources/test_executable_cache.py	19278
test_replicated_merge_tree_wait_on_shutdown/test.py	19164
test_replicated_access/test.py	18999
test_ttl_multilevel_group_by/test.py	18822
test_s3_cluster/test.py	18798
test_keeper_reconfig_replace_leader/test.py	18551
test_dictionaries_wait_for_load/test.py	18462
test_file_schema_inference_cache/test.py	18423
test_storage_iceberg_with_spark/test_delete_files.py	18175
test_storage_iceberg_disks/test.py	18123
test_backward_compatibility/test_vertical_merges_from_compact_parts.py	18078
test_storage_kafka/test_intent_sizes.py	18019
test_zookeeper_config_load_balancing/test.py	17863
test_shutdown_wait_unfinished_queries/test.py	17709
test_grant_and_revoke/test_with_table_engine_grant.py	17626
test_modify_engine_on_restart/test_storage_policies.py	17612
test_keeper_snapshot_on_exit/test.py	17555
test_storage_iceberg_with_spark/test_writes.py	17440
test_library_bridge/test_exiled.py	17433
test_zookeeper_connection_log/test.py	17275
test_https_replication/test.py	16986
test_secure_socket/test.py	16975
test_database_glue/test.py	16726
test_kafka_bad_messages/test_1.py	16604
test_lightweight_updates/test.py	16514
test_cancel_freeze/test.py	16494
test_warning_broken_tables/test.py	16358
test_attach_partition_using_copy/test.py	16317
test_storage_iceberg_with_spark/test_iceberg_snapshot_reads.py	15980
test_insert_into_distributed/test.py	15956
test_attach_without_fetching/test.py	15897
test_dictionaries_mysql/test.py	15821
test_startup_scripts/test.py	15567
test_manipulate_statistics/test.py	15547
test_storage_s3/test_invalid_env_credentials.py	15033
test_storage_delta_disks/test.py	14977
test_suggestions/test.py	14883
test_backup_restore/test.py	14829
test_check_table/test.py	14726
test_mutations_with_merge_tree/test.py	14474
test_search_orphaned_parts/test.py	14436
test_keeper_reconfig_remove/test.py	14415
test_quota/test.py	14263
test_allow_feature_tier/test.py	14197
test_parallel_replicas_custom_key/test.py	13944
test_acme_tls/test_single_node.py	13767
test_quorum_inserts_parallel/test.py	13754
test_storage_s3_queue/test_parallel_inserts.py	13741
test_reload_zookeeper/test.py	13594
test_keeper_four_word_command/test.py	13515
test_partition/test.py	13509
test_replace_partition/test.py	13247
test_https_replication/test_change_ip.py	12749
test_format_schema_source/test.py	12635
test_multi_access_storage_role_management/test.py	12609
test_zookeeper_config/test_secure.py	12567
test_storage_iceberg_no_spark/test_writes_multiple_files.py	12486
test_disk_checker/test.py	12462
test_move_partition_to_volume_async/test.py	12440
test_storage_kafka/test_schema_registry_skip_bytes.py	12400
test_storage_iceberg_schema_evolution/test_map_evolved_nested.py	12399
test_enable_user_name_access_type/test.py	12325
test_grpc_protocol/test.py	12276
test_parts_delete_zookeeper/test.py	12178
test_join_set_family_s3/test.py	12083
test_recovery_replica/test.py	12069
test_replicated_fetches_timeouts/test.py	11994
test_keeper_reconfig_remove_many/test.py	11917
test_scheduler_cpu/test.py	11887
test_access_for_functions/test.py	11861
test_modify_engine_on_restart/test_zk_path_exists.py	11761
test_keeper_session/test.py	11549
test_log_lz4_streaming/test.py	11500
test_compressed_marks_restart/test.py	11497
test_storage_delta/test_cdf.py	11435
test_keeper_three_nodes_start/test.py	11400
test_sql_roles_for_xml_users/test.py	11328
test_dictionaries_replace/test.py	11282
test_recovery_time_metric/test.py	11241
test_keeper_remove_acl/test.py	11217
test_keeper_persistent_log_multinode/test.py	11181
test_default_compression_codec/test.py	11150
test_reload_auxiliary_zookeepers/test.py	11107
test_storage_iceberg_with_spark/test_writes_schema_evolution.py	11099
test_storage_delta/test_imds.py	10973
test_keeper_persistent_log/test.py	10905
test_distributed_inter_server_secret/test.py	10846
test_interserver_dns_retires/test.py	10808
test_keeper_mntr_data_size/test.py	10762
test_config_yaml_full/test.py	10729
test_config_reloader_interval/test.py	10689
test_transactions/test.py	10681
test_config_xml_yaml_mix/test.py	10582
test_trace_log_build_id/test.py	10580
test_s3_storage_conf_new_proxy/test.py	10491
test_table_db_num_limit/test.py	10454
test_rocksdb_read_only/test.py	10438
test_replicated_merge_tree_compatibility/test.py	10428
test_storage_iceberg_with_spark/test_writes_create_table.py	10363
test_distributed_default_database/test.py	10314
test_replication_without_zookeeper/test.py	10279
test_lazy_database/test.py	10276
test_replication_credentials/test.py	10266
test_config_yaml_main/test.py	10216
test_system_start_stop_listen/test.py	10127
test_force_restore_data_flag_for_keeper_dataloss/test.py	10021
test_trace_collector_serverwide/test.py	9990
test_storage_iceberg_with_spark/test_writes_with_partitioned_table.py	9935
test_config_xml_full/test.py	9922
test_system_ddl_worker_queue/test.py	9876
test_server_start_and_ip_conversions/test.py	9710
test_parallel_replicas_failover/test.py	9675
test_backup_restore_on_cluster_with_checksum_data_file_name/test.py	9581
test_startup_scripts_execution_state/test.py	9564
test_storage_iceberg_with_spark/test_explanation.py	9277
test_storage_policies/test.py	9224
test_config_xml_main/test.py	9223
test_keeper_snapshots_multinode/test.py	9213
test_hot_reload_storage_policy/test.py	9179
test_version_update/test.py	9176
test_server_reload/test.py	9112
test_storage_iceberg_with_spark/test_minmax_pruning_with_null.py	9100
test_dictionaries_redis/test_long.py	9045
test_validate_threadpool_writer_pool_size/test.py	9003
test_filesystem_cache_uninitialized/test.py	8954
test_config_yaml_merge_keys/test.py	8953
test_user_defined_object_persistence/test.py	8946
test_settings_profile/test.py	8920
test_remote_blobs_naming/test_backward_compatibility.py	8906
test_allowed_url_from_config/test.py	8808
test_drop_replica_with_auxiliary_zookeepers/test.py	8770
test_system_reload_async_metrics/test_async_metrics_invalid_settings.py	8653
test_keeper_profiler/test.py	8620
test_replica_is_active/test.py	8498
test_config_substitutions/test.py	8318
test_s3_access_headers/test.py	8260
test_ddl_worker_non_leader/test.py	8256
test_async_insert_adaptive_busy_timeout/test.py	8256
test_mark_cache_profile_events/test.py	8136
test_insert_into_distributed_sync_async/test.py	7919
test_match_process_uid_against_data_owner/test.py	7906
test_replicated_merge_tree_encryption_codec/test.py	7852
test_select_access_rights/test_main.py	7817
test_storage_iceberg_with_spark/test_optimize.py	7733
test_consistant_parts_after_move_partition/test.py	7578
test_always_fetch_merged/test.py	7575
test_stop_insert_when_disk_close_to_full/test.py	7503
test_distributed_ddl/test_replicated_alter.py	7437
test_keeper_restore_from_snapshot/test_disk_s3.py	7387
test_format_cannot_allocate_thread/test.py	7357
test_parallel_replicas_distributed_skip_shards/test.py	7285
test_asynchronous_metrics_pk_bytes_fields/test.py	7282
test_keeper_memory_soft_limit/test.py	7279
test_keeper_map_retries/test.py	7181
test_storage_iceberg_with_spark/test_bucket_partition_pruning.py	7158
test_storage_iceberg_with_spark/test_restart_broken_s3.py	7130
test_insert_distributed_load_balancing/test.py	7114
test_merge_tree_s3_with_cache/test.py	7043
test_dictionary_asynchronous_metrics/test.py	7005
test_keeper_restore_from_snapshot/test.py	6954
test_sharding_key_from_default_column/test.py	6947
test_zookeeper_fallback_session/test.py	6929
test_backup_restore_azure_blob_storage/test.py	6924
test_prometheus_protocols/test_write_read.py	6916
test_temporary_data/test.py	6878
test_placement_info/test.py	6874
test_modify_engine_on_restart/test_unusual_path.py	6803
test_extreme_deduplication/test.py	6791
test_system_queries/test.py	6692
test_storage_numbers/test.py	6598
test_database_iceberg_nessie_catalog/test.py	6558
test_storage_iceberg_with_spark/test_multiple_iceberg_file.py	6466
test_backup_log/test.py	6453
test_storage_iceberg_schema_evolution/test_correct_column_mapper_is_chosen.py	6444
test_restart_server/test.py	6426
test_aliases_in_default_expr_not_break_table_structure/test.py	6296
test_fix_metadata_version/test.py	6266
test_distributed_format/test.py	6248
test_filesystem_layout/test.py	6241
test_storage_iceberg_with_spark/test_partition_by.py	6097
test_modify_engine_on_restart/test_mv.py	6071
test_threadpool_readers/test.py	6040
test_storage_iceberg_with_spark/test_writes_from_zero.py	6005
test_profile_max_sessions_for_user/test.py	5869
test_table_function_mongodb/test.py	5786
test_non_default_compression/test.py	5769
test_dictionary_allow_read_expired_keys/test_default_reading.py	5660
test_dictionary_allow_read_expired_keys/test_dict_get_or_default.py	5611
test_dictionary_allow_read_expired_keys/test_dict_get.py	5608
test_modify_engine_on_restart/test_args.py	5607
test_session_settings_table/test.py	5557
test_graphite_merge_tree_typed/test.py	5528
test_disks_app_func/test.py	5524
test_permissions_drop_replica/test.py	5517
test_insert_distributed_async_extra_dirs/test.py	5445
test_replicated_merge_tree_s3/test.py	5444
test_max_suspicious_broken_parts_replicated/test.py	5423
test_concurrent_threads_soft_limit/test.py	5421
test_keeper_watches/test.py	5288
test_reload_max_table_size_to_drop/test.py	5265
test_cleanup_after_start/test.py	5259
test_read_temporary_tables_on_failure/test.py	5251
test_grpc_protocol_ssl/test.py	5223
test_backward_compatibility/test_ip_types_binary_compatibility.py	5200
test_dictionary_allow_read_expired_keys/test_default_string.py	5178
test_storage_redis/test.py	5172
test_send_request_to_leader_replica/test.py	5109
test_attach_with_different_projections_or_indices/test.py	5101
test_graphite_merge_tree/test.py	5082
test_backup_restore_on_cluster/test_huge_concurrent_restore.py	5066
test_plain_rewr_legacy_layout/test.py	5000
test_restart_with_unavailable_azure/test.py	4931
test_keeper_reconfig_add/test.py	4921
test_force_drop_table/test.py	4921
test_limit_materialized_view_count/test.py	4869
test_mutations_in_partitions_of_merge_tree/test.py	4864
test_temporary_data_in_cache/test.py	4820
test_log_family_s3/test.py	4819
test_reload_clusters_config/test.py	4773
test_mutation_fetch_fallback/test.py	4725
test_detached_parts_metrics/test.py	4714
test_settings_constraints/test.py	4707
test_storage_iceberg_with_spark/test_writes_complex_type.py	4693
test_alter_database_on_cluster/test.py	4625
test_server_startup_and_shutdown_logs/test.py	4605
test_disabled_access_control_improvements/test_row_policy.py	4599
test_shutdown_static_destructor_failure/test.py	4584
test_storage_iceberg_with_spark/test_types.py	4573
test_create_user_and_login/test.py	4545
test_max_suspicious_broken_parts/test.py	4484
test_drop_replica/test.py	4372
test_backup_restore_storage_policy/test.py	4339
test_reload_client_certificate/test.py	4331
test_access_control_on_cluster/test.py	4286
test_insert_into_distributed_through_materialized_view/test.py	4285
test_backward_compatibility/test_rocksdb_upgrade.py	4276
test_storage_iceberg_with_spark/test_read_in_order.py	4221
test_reloading_settings_from_users_xml/test.py	4216
test_storage_azure_blob_storage/test_cluster.py	4191
test_replicated_merge_tree_encrypted_disk/test.py	4165
test_part_log_table/test.py	4122
test_ldap_external_user_directory/test.py	4030
test_system_logs_comment/test.py	4002
test_cross_replication/test.py	3935
test_jemalloc_percpu_arena/test.py	3919
test_distributed_async_insert_for_node_changes/test.py	3897
test_async_connect_to_multiple_ips/test.py	3880
test_storage_iceberg_with_spark/test_metadata_cache.py	3845
test_no_merges_volume_ttl/test.py	3818
test_overcommit_tracker/test.py	3800
test_prometheus_protocols/test_different_table_engines.py	3755
test_storage_kafka/test_zookeeper_locks.py	3751
test_dictionaries_select_all/test.py	3716
test_backward_compatibility/test_block_marshalling.py	3713
test_dictionary_ddl_on_cluster/test.py	3712
test_covered_by_broken_exists/test.py	3710
test_alter_on_mixed_type_cluster/test.py	3709
test_session_log/test.py	3649
test_azure_blob_storage_native_copy/test.py	3587
test_storage_s3/test_sts.py	3573
test_distributed_insert_backward_compatibility/test.py	3491
test_sync_replica_on_cluster/test.py	3483
test_merge_tree_check_part_with_cache/test.py	3476
test_trace_log_memory_context/test.py	3468
test_tcp_handler_connection_limits/test.py	3444
test_backup_restore_on_cluster/test_slow_rmt.py	3436
test_replicated_merge_tree_thread_schedule_timeouts/test.py	3432
test_fetch_memory_usage/test.py	3397
test_os_thread_nice_value/test.py	3375
test_arrowflight_interface/test_ticket_expiration.py	3339
test_build_sets_from_multiple_threads/test.py	3338
test_distributed_ddl_password/test.py	3280
test_keeper_dynamic_log_level/test.py	3271
test_attach_partition_with_large_destination/test.py	3237
test_backup_restore_s3/test_throttling.py	3217
test_cgroup_limit/test.py	3202
test_storage_iceberg_with_spark/test_partition_pruning_with_subquery_set.py	3197
test_storage_iceberg_with_spark/test_single_iceberg_file.py	3177
test_settings_constraints_distributed/test.py	3162
test_runtime_configurable_cache_size/test.py	3127
test_asynchronous_metric_log_table/test.py	3123
test_mutations_with_projection/test.py	3120
test_cluster_discovery/test_password.py	3046
test_zookeeper_config/test.py	3045
test_parallel_replicas_snapshot_from_initiator/test.py	2980
test_global_overcommit_tracker/test.py	2964
test_backup_restore_on_cluster/test_two_shards_two_replicas.py	2960
test_insert_over_http_query_log/test.py	2936
test_disable_insertion_and_mutation/test.py	2933
test_force_deduplication/test.py	2931
test_http_connection_drain_before_reuse/test.py	2915
test_ddl_alter_query/test.py	2886
test_parallel_replicas_protocol/test.py	2813
test_arrowflight_interface/test.py	2805
test_storage_iceberg_with_spark/test_writes_multiple_threads.py	2798
test_select_access_rights/test_from_system_tables.py	2782
test_compression_nested_columns/test.py	2765
test_storage_iceberg_with_spark/test_writes_create_version_hint.py	2765
test_git_import/test.py	2742
test_storage_iceberg_no_spark/test_writes_multiple_threads.py	2641
test_ssl_cert_authentication/test.py	2574
test_postgresql_protocol/test.py	2567
test_input_format_parallel_parsing_memory_tracking/test.py	2560
test_dremio_engine/test.py	2556
test_attach_table_normalizer/test.py	2542
test_dotnet_client/test.py	2532
test_s3_cluster_insert_select/test.py	2518
test_system_reconnect_zookeeper/test.py	2427
test_arrowflight_storage/test.py	2423
test_send_crash_reports/test.py	2383
test_storage_iceberg_with_spark/test_writes_drop_table.py	2378
test_distributed_ddl_on_cross_replication/test.py	2333
test_profile_events_s3/test.py	2316
test_database_iceberg_lakekeeper_catalog/test.py	2302
test_compatibility_merge_tree_settings/test.py	2262
test_settings_from_server/test.py	2212
test_dictionaries_access/test.py	2209
test_disabled_access_control_improvements/test_select_from_system_tables.py	2202
test_analyzer_compatibility/test.py	2193
test_validate_only_initial_alter_query/test_replicated_database.py	2178
test_storage_url/test.py	2167
test_check_table_name_length_2/test.py	2152
test_database_hms/test.py	2149
test_parallel_replicas_no_replicas/test.py	2148
test_wrong_db_or_table_name/test.py	2114
test_file_cluster/test.py	2064
test_cow_policy/test.py	2064
test_external_http_authenticator/test.py	2043
test_broken_part_during_merge/test.py	2023
test_ddl_config_hostname/test.py	1980
test_jbod_load_balancing/test.py	1953
test_s3_low_cardinality_right_border/test.py	1927
test_drop_if_empty/test.py	1925
test_ddl_worker_with_loopback_hosts/test.py	1912
test_reload_certificate/test.py	1895
test_default_role/test.py	1877
test_part_uuid/test.py	1876
test_storage_iceberg_with_spark/test_minmax_pruning_for_arrays_and_maps_subfields_disabled.py	1872
test_system_logs_hostname/test_replicated.py	1842
test_storage_iceberg_with_spark/test_writes_field_partitioning.py	1841
test_materialized_view_restart_server/test.py	1838
test_storage_iceberg_with_spark/test_relevant_iceberg_schema_chosen.py	1829
test_create_query_constraints/test.py	1779
test_default_compression_in_mergetree_settings/test.py	1768
test_ddl_worker_retry_when_dropping_db_failed/test.py	1724
test_prometheus_protocols/test_evaluation.py	1682
test_storage_iceberg_with_spark/test_filesystem_cache.py	1639
test_storage_iceberg_schema_evolution/test_full_drop.py	1633
test_keeper_four_word_command/test_allow_list.py	1589
test_executable_udf_names_in_system_query_log/test.py	1587
test_memory_profiler_min_max_borders/test.py	1576
test_backward_compatibility/test_parallel_replicas_protocol.py	1569
test_attach_backup_from_s3_plain/test.py	1534
test_encrypted_disk_replication/test.py	1531
test_matview_union_replicated/test.py	1515
test_fetch_partition_should_reset_mutation/test.py	1497
test_external_cluster/test.py	1492
test_keeper_max_request_size/test.py	1482
test_table_functions_access_rights/test.py	1466
test_storage_iceberg_no_spark/test_read_in_order_with_pyiceberg.py	1464
test_replicated_s3_zero_copy_drop_partition/test.py	1444
test_format_schema_on_server/test.py	1437
test_storage_iceberg_with_spark/test_compressed_metadata.py	1421
test_deduplicated_attached_part_rename/test.py	1375
test_distributed_over_distributed/test.py	1336
test_concurrent_queries_for_all_users_restriction/test.py	1330
test_keeper_http_control/test.py	1321
test_truncate_database/test_distributed.py	1296
test_backward_compatibility/test_memory_bound_aggregation.py	1278
test_optimize_on_insert/test.py	1263
test_distributed_storage_configuration/test.py	1253
test_merge_tree_load_marks/test.py	1238
test_max_authentication_methods_per_user/test.py	1229
test_default_database_on_cluster/test.py	1216
test_log_levels_update/test.py	1210
test_drop_no_local_path/test.py	1188
test_alternative_keeper_config/test.py	1179
test_shard_names/test.py	1177
test_alter_settings_on_cluster/test.py	1164
test_backward_compatibility/test_const_node_optimization.py	1160
test_replicated_database_interserver_host/test.py	1143
test_parallel_replicas_increase_error_count/test.py	1135
test_concurrent_queries_for_user_restriction/test.py	1135
test_codec_encrypted/test.py	1133
test_replicated_database_alter_modify_order_by/test.py	1126
test_timezone_config/test.py	1125
test_truncate_database/test_replicated.py	1117
test_allowed_client_hosts/test.py	1114
test_fetch_partition_from_auxiliary_zookeeper/test.py	1105
test_alter_comment_on_cluster/test.py	1101
test_move_partition_to_disk_on_cluster/test.py	1097
test_format_avro_confluent/test.py	1091
test_password_constraints/test.py	1073
test_text_log_level/test.py	1046
test_merge_table_over_distributed/test.py	1042
test_storage_iceberg_no_spark/test_time_travel_bug_fix_validation.py	1041
test_backup_restore_on_cluster/test_different_versions.py	1013
test_merge_tree_prewarm_cache/test.py	1004
test_attach_table_from_s3_plain_readonly/test.py	995
test_attach_without_checksums/test.py	984
test_storage_iceberg_with_spark/test_writes_different_path_format_error.py	979
test_s3_imds/test_simple.py	975
test_failed_async_inserts/test.py	971
test_intersecting_parts/test.py	971
test_sql_user_defined_functions_on_cluster/test.py	966
test_prefer_global_in_and_join/test.py	956
test_disk_name_virtual_column/test.py	939
test_composable_protocols/test.py	928
test_peak_memory_usage/test.py	920
test_cluster_all_replicas/test.py	916
test_table_function_redis/test.py	910
test_merge_tree_empty_parts/test.py	898
test_s3_style_link/test.py	897
test_replicated_detach_table/test.py	890
test_user_zero_database_access/test_user_zero_database_access.py	879
test_ssh/test.py	856
test_backward_compatibility/test_aggregation_with_out_of_order_buckets.py	854
test_structured_logging_json/test.py	852
test_s3_imds/test_session_token.py	825
test_aggregation_memory_efficient/test.py	821
test_groupBitmapAnd_on_distributed/test.py	810
test_tlsv1_3/test.py	808
test_replicated_parse_zk_metadata/test.py	803
test_prometheus_endpoint/test.py	799
test_dictionary_custom_settings/test.py	799
test_ssh_keys_authentication/test.py	786
test_grant_and_revoke/test_without_table_engine_grant.py	777
test_s3_with_https/test.py	775
test_groupBitmapAnd_on_distributed/test_groupBitmapAndState_on_distributed_table.py	770
test_storage_dict/test.py	769
test_freeze_table/test.py	741
test_storage_iceberg_no_spark/test_writes_nullable_bugs2.py	741
test_kerberos_auth/test.py	734
test_fetch_partition_with_outdated_parts/test.py	733
test_check_table_name_length/test.py	720
test_storage_iceberg_with_spark/test_pruning_nullable_bug.py	713
test_storage_url_http_headers/test.py	709
test_backward_compatibility/test_short_strings_aggregation.py	704
test_dot_in_user_name/test.py	699
test_replicated_engine_arguments/test.py	690
test_replicated_merge_tree_replicated_db_ttl/test.py	684
test_backward_compatibility/test_select_aggregate_alias_column.py	671
test_old_versions/test.py	650
test_distributed_config/test.py	639
test_sql_user_impersonate/test.py	635
test_storage_iceberg_no_spark/test_writes_with_compression_metadata.py	620
test_backward_compatibility/test_normalized_count_comparison.py	618
test_reload_query_masking_rules/test.py	612
test_geoparquet/test.py	597
test_accept_invalid_certificate/test.py	596
test_user_ip_restrictions/test.py	588
test_server_keep_alive/test.py	581
test_database_disk/test.py	580
test_user_grants_from_config/test.py	577
test_custom_settings/test.py	575
test_config_hide_in_preprocessed/test.py	551
test_merge_tree_settings_constraints/test.py	540
test_backward_compatibility/test_aggregate_fixed_key.py	538
test_disabled_mysql_server/test.py	526
test_storage_azure_blob_storage/test_check_after_upload.py	525
test_enabling_access_management/test.py	519
test_parallel_replicas_skip_shards/test.py	512
test_unknown_column_dist_table_with_alias/test.py	510
test_distributed_system_query/test.py	507
test_replicated_database/test_settings_recover_lost_replica.py	502
test_system_reload_async_metrics/test.py	502
test_storage_s3_intelligent_tier/test.py	493
test_parquet_page_index/test.py	491
test_zero_copy_expand_macros/test.py	460
test_inherit_multiple_profiles/test.py	425
test_server_initialization/test.py	413
test_replicated_merge_tree_config/test.py	410
test_dictionaries_with_invalid_structure/test.py	408
test_authentication/test.py	388
test_dictionaries_null_value/test.py	373
test_explain_estimates/test.py	373
test_alter_update_cast_keep_nullable/test.py	360
test_buffer_profile/test.py	359
test_config_decryption/test.py	358
test_keeper_path_acl/test.py	345
test_storage_iceberg_with_spark/test_cluster_table_function_with_partition_pruning.py	310
test_custom_dashboards/test.py	309
test_backward_compatibility/test_insert_profile_events.py	308
test_backup_s3_storage_class/test.py	297
test_internal_queries_not_counted/test.py	294
test_disks_app_interactive/test.py	293
test_range_hashed_dictionary_types/test.py	286
test_scram_sha256_password_with_replicated_zookeeper_replicator/test.py	261
test_disk_types/test.py	258
test_block_structure_mismatch/test.py	244
test_settings_randomization/test.py	244
test_s3_storage_class/test.py	244
test_storage_url_with_proxy/test.py	235
test_passing_max_partitions_to_read_remotely/test.py	230
test_backward_compatibility/test_cte_distributed.py	229
test_host_regexp_hosts_file_resolution/test.py	228
test_render_log_file_name_templates/test.py	225
test_relative_filepath/test.py	211
test_max_rows_to_read_leaf_with_view/test.py	194
test_backward_compatibility/test.py	194
test_endpoint_macro_substitution/test.py	194
test_storage_iceberg_no_spark/test_graceful_error_not_configured_iceberg_metadata_log.py	180
test_profile_settings_and_constraints_order/test.py	180
test_storage_iceberg_no_spark/test_writes_create_table_bugs.py	180
test_remote_function_view/test.py	180
test_unambiguous_alter_commands/test.py	179
test_delayed_remote_source/test.py	165
test_keeper_compression/test_with_compression.py	165
test_backward_compatibility/test_old_client_with_replicated_columns.py	165
test_disks_app_other_disk_types/test.py	157
test_async_logger_metrics/test.py	132
test_replica_can_become_leader/test.py	130
test_http_native/test.py	123
test_keeper_metrics_only/test.py	118
test_remote_prewhere/test.py	118
test_replicating_constants/test.py	115
test_keeper_and_access_storage/test.py	115
test_shard_level_const_function/test.py	115
test_logs_level/test.py	115
test_tcp_handler_http_responses/test_case.py	114
test_http_and_readonly/test.py	111
test_keeper_ipv4_fallback/test.py	107
test_keeper_availability_zone/test.py	103
test_keeper_secure_client/test.py	70
test_config_decryption/test_zk_secure.py	67
test_union_header/test.py	65
test_keeper_compression/test_without_compression.py	65
test_config_decryption/test_zk.py	65
test_ssh/test_options_propagation_enabled.py	61
test_keeper_client/test.py	43
test_tcp_handler_interserver_listen_host/test_case.py	4
test_config_corresponding_root/test.py	0
test_userspace_page_cache/test_incorrect_limits.py	0
test_concurrent_backups_s3/test.py	0
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


def get_optimal_test_batch(
    tests: list[str], total_batches: int, batch_num: int, num_workers: int
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
        if any(test_file.startswith(test_config.prefix) for test_config in TEST_CONFIGS)
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

    # Compute group durations as sum of known test durations within the group
    def groups_with_durations(groups: dict[str, list[str]]):
        known_groups: list[tuple[str, int]] = []  # (prefix, duration)
        unknown_groups: list[str] = []  # prefixes with zero known duration
        for prefix, items in sorted(groups.items()):
            dur = sum(TEST_DURATIONS.get(t, 0) for t in items)
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

    # Prepare batch containers and weights
    parallel_batches: list[list[str]] = [[] for _ in range(total_batches)]
    parallel_weights: list[int] = [0] * total_batches

    # LPT assign known-duration parallel groups
    for prefix, dur in p_known:
        idx = min(range(total_batches), key=lambda i: (parallel_weights[i], i))
        # prefix, dur sorted in p_known starting with longest duration - keep the order in batches to decrease tail latency
        parallel_batches[idx].extend(parallel_groups[prefix])
        parallel_weights[idx] += dur

    # Sort tests within each batch by duration (longest first) to minimize tail latency
    # when tests are picked by workers from the queue
    for idx in range(total_batches):
        parallel_batches[idx].sort(key=lambda x: (-TEST_DURATIONS.get(x, 0), x))

    # Round-robin assign unknown-duration parallel groups
    for i, prefix in enumerate(p_unknown):
        idx = i % total_batches
        parallel_batches[idx].extend(parallel_groups[prefix])

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

    print(
        f"Batches parallel weights: [{[weight // num_workers // 1000 for weight in parallel_weights]}]"
    )
    print(f"Batches weights: [{[weight // 1000 for weight in sequential_weights]}]")

    # Sanity check (non-fatal): ensure total test count preserved
    total_assigned = sum(len(b) for b in parallel_batches) + sum(
        len(b) for b in sequential_batches
    )
    assert total_assigned == len(tests)

    return parallel_batches[batch_num - 1], sequential_batches[batch_num - 1]
