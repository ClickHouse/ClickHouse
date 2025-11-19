import dataclasses


@dataclasses.dataclass
class TC:
    prefix: str
    is_sequential: bool
    comment: str


TEST_CONFIGS = [
    TC("test_atomic_drop_table/", True, "no idea why i'm sequential"),
    TC("test_attach_without_fetching/", True, "no idea why i'm sequential"),
    TC(
        "test_cleanup_dir_after_bad_zk_conn/",
        True,
        "no idea why i'm sequential",
    ),
    TC(
        "test_consistent_parts_after_clone_replica/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_crash_log/", True, "no idea why i'm sequential"),
    TC("test_cross_replication/", True, "no idea why i'm sequential"),
    TC("test_ddl_worker_non_leader/", True, "no idea why i'm sequential"),
    TC("test_delayed_replica_failover/", True, "no idea why i'm sequential"),
    TC(
        "test_dictionary_allow_read_expired_keys/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_disabled_mysql_server/", True, "no idea why i'm sequential"),
    TC("test_distributed_respect_user_timeouts/", True, "no idea why i'm sequential"),
    TC("test_dns_cache/", True, "no idea why i'm sequential"),
    TC("test_global_overcommit_tracker/", True, "no idea why i'm sequential"),
    TC("test_grpc_protocol/", True, "no idea why i'm sequential"),
    TC("test_host_regexp_multiple_ptr_records/", True, "no idea why i'm sequential"),
    TC("test_http_failover/", True, "no idea why i'm sequential"),
    TC("test_https_replication/", True, "no idea why i'm sequential"),
    TC("test_insert_into_distributed/", True, "no idea why i'm sequential"),
    TC(
        "test_insert_into_distributed_through_materialized_view/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_keeper_map/", True, "no idea why i'm sequential"),
    TC("test_keeper_multinode_simple/", True, "no idea why i'm sequential"),
    TC("test_keeper_two_nodes_cluster/", True, "no idea why i'm sequential"),
    TC("test_limited_replicated_fetches/", True, "no idea why i'm sequential"),
    TC("test_merge_tree_s3/", True, "no idea why i'm sequential"),
    TC("test_mysql_database_engine/", True, "no idea why i'm sequential"),
    TC("test_parts_delete_zookeeper/", True, "no idea why i'm sequential"),
    TC(
        "test_postgresql_replica_database_engine/",
        True,
        "no idea why i'm sequential",
    ),
    TC(
        "test_profile_max_sessions_for_user/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_quorum_inserts_parallel/", True, "no idea why i'm sequential"),
    TC("test_random_inserts/", True, "no idea why i'm sequential"),
    TC("test_replace_partition/", True, "no idea why i'm sequential"),
    TC("test_replicated_database/", True, "no idea why i'm sequential"),
    TC("test_replicated_fetches_timeouts/", True, "no idea why i'm sequential"),
    TC(
        "test_replicated_merge_tree_wait_on_shutdown/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_server_overload/", True, "no idea why i'm sequential"),
    TC("test_server_reload/", True, "no idea why i'm sequential"),
    TC("test_storage_kafka/", True, "no idea why i'm sequential"),
    TC("test_storage_kerberized_kafka/", True, "no idea why i'm sequential"),
    TC("test_storage_rabbitmq/", True, "no idea why i'm sequential"),
    TC("test_storage_s3/", True, "no idea why i'm sequential"),
    TC("test_storage_s3_queue/", True, "no idea why i'm sequential"),
    TC("test_system_flush_logs/", True, "no idea why i'm sequential"),
    TC("test_system_logs/", True, "no idea why i'm sequential"),
    TC("test_system_metrics/", True, "no idea why i'm sequential"),
    TC("test_ttl_move/", True, "no idea why i'm sequential"),
    TC("test_user_ip_restrictions/", True, "no idea why i'm sequential"),
    TC(
        "test_zookeeper_config_load_balancing/",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_zookeeper_fallback_session/", True, "no idea why i'm sequential"),
    TC(
        "test_backup_restore_on_cluster/test_concurrency.py",
        True,
        "no idea why i'm sequential",
    ),
    TC("test_database_delta/test.py", True, "no idea why i'm sequential"),
    TC("test_keeper_ipv4_fallback/test.py", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_no_spark/", True, "no idea why i'm sequential"),
    TC("test_storage_iceberg_with_spark/", True, "no idea why i'm sequential"),
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
}

# collected by
# SELECT
#     splitByString('::', test_name)[1] AS test_file,
#     SUM(test_duration_ms) AS dur
# FROM checks
# WHERE 1
#   AND check_name LIKE 'Integration tests%'
#   AND commit_sha = '9d5e859570b9b281c0a858f85dcee58d58fb8399'
# GROUP BY test_file
# ORDER BY test_file
RAW_TEST_DURATIONS = """
test_MemoryTracking/test.py	339540
test_accept_invalid_certificate/test.py	27980
test_access_control_on_cluster/test.py	87740
test_access_for_functions/test.py	42680
test_aggregation_memory_efficient/test.py	50520
test_aliases_in_default_expr_not_break_table_structure/test.py	92730
test_allow_feature_tier/test.py	119360
test_allowed_client_hosts/test.py	47480
test_allowed_url_from_config/test.py	93250
test_alter_codec/test.py	48480
test_alter_comment_on_cluster/test.py	75970
test_alter_database_on_cluster/test.py	154760
test_alter_moving_garbage/test.py	310630
test_alter_on_mixed_type_cluster/test.py	120080
test_alter_settings_on_cluster/test.py	76950
test_alter_update_cast_keep_nullable/test.py	63390
test_alternative_keeper_config/test.py	144810
test_always_fetch_merged/test.py	197710
test_analyzer_compatibility/test.py	130930
test_arrowflight_interface/test.py	60520
test_arrowflight_storage/test.py	72520
test_async_connect_to_multiple_ips/test.py	71670
test_async_insert_adaptive_busy_timeout/test.py	98098
test_async_insert_memory/test.py	267160
test_async_load_databases/test.py	1051810
test_async_logger_metrics/test.py	28289
test_async_metrics_in_cgroup/test.py	86180
test_asynchronous_metric_log_table/test.py	65700
test_asynchronous_metrics_pk_bytes_fields/test.py	114580
test_atomic_drop_table/test.py	144560
test_attach_backup_from_s3_plain/test.py	53390
test_attach_partition_using_copy/test.py	157390
test_attach_partition_with_large_destination/test.py	100080
test_attach_table_from_s3_plain_readonly/test.py	75590
test_attach_table_normalizer/test.py	32980
test_attach_with_different_projections_or_indices/test.py	62980
test_attach_without_checksums/test.py	31810
test_attach_without_fetching/test.py	190040
test_authentication/test.py	27290
test_azure_blob_storage_native_copy/test.py	80800
test_azure_blob_storage_plain_rewritable/test.py	304840
test_backup_log/test.py	52960
test_backup_restore/test.py	101560
test_backup_restore_azure_blob_storage/test.py	64980
test_backup_restore_keeper_map/test.py	239000
test_backup_restore_new/test.py	1216847
test_backup_restore_new/test_cancel_backup.py	619490
test_backup_restore_new/test_shutdown_wait_backup.py	199780
test_backup_restore_on_cluster/test.py	669046
test_backup_restore_on_cluster/test_cancel_backup.py	526500
test_backup_restore_on_cluster/test_concurrency.py	2121370
test_backup_restore_on_cluster/test_different_versions.py	185440
test_backup_restore_on_cluster/test_disallow_concurrency.py	369760
test_backup_restore_on_cluster/test_slow_rmt.py	158450
test_backup_restore_on_cluster/test_two_shards_two_replicas.py	99670
test_backup_restore_s3/test.py	1171860
test_backup_restore_s3/test_throttling.py	61230
test_backup_restore_storage_policy/test.py	57340
test_backup_s3_storage_class/test.py	68480
test_backward_compatibility/test.py	67420
test_backward_compatibility/test_aggregate_fixed_key.py	107059
test_backward_compatibility/test_aggregate_function_state.py	953340
test_backward_compatibility/test_aggregation_with_out_of_order_buckets.py	116560
test_backward_compatibility/test_block_marshalling.py	188680
test_backward_compatibility/test_convert_ordinary.py	540830
test_backward_compatibility/test_cte_distributed.py	55740
test_backward_compatibility/test_functions.py	382570
test_backward_compatibility/test_insert_profile_events.py	25669
test_backward_compatibility/test_ip_types_binary_compatibility.py	160040
test_backward_compatibility/test_memory_bound_aggregation.py	45030
test_backward_compatibility/test_normalized_count_comparison.py	27819
test_backward_compatibility/test_parallel_replicas_protocol.py	152910
test_backward_compatibility/test_rocksdb_upgrade.py	112770
test_backward_compatibility/test_select_aggregate_alias_column.py	27240
test_backward_compatibility/test_short_strings_aggregation.py	72550
test_backward_compatibility/test_vertical_merges_from_compact_parts.py	256340
test_block_structure_mismatch/test.py	84459
test_broken_part_during_merge/test.py	67560
test_broken_projections/test.py	376059
test_buffer_profile/test.py	26790
test_build_sets_from_multiple_threads/test.py	47300
test_cancel_freeze/test.py	229240
test_catboost_evaluate/test.py	160740
test_cgroup_limit/test.py	13680
test_check_table/test.py	167070
test_check_table_name_length/test.py	79400
test_check_table_name_length_2/test.py	66210
test_checking_s3_blobs_paranoid/test.py	813859
test_cleanup_after_start/test.py	67120
test_cleanup_dir_after_bad_zk_conn/test.py	375530
test_cluster_all_replicas/test.py	44760
test_cluster_discovery/test.py	288790
test_cluster_discovery/test_auxiliary_keeper.py	260790
test_cluster_discovery/test_dynamic_clusters.py	265190
test_cluster_discovery/test_password.py	125490
test_codec_encrypted/test.py	36180
test_compatibility_merge_tree_settings/test.py	67710
test_composable_protocols/test.py	20020
test_compressed_marks_restart/test.py	63780
test_compression_nested_columns/test.py	68100
test_concurrent_queries_for_all_users_restriction/test.py	40070
test_concurrent_queries_for_user_restriction/test.py	40540
test_concurrent_queries_restriction_by_query_kind/test.py	151360
test_concurrent_threads_soft_limit/test.py	76240
test_concurrent_ttl_merges/test.py	899460
test_config_corresponding_root/test.py	6220
test_config_decryption/test.py	26240
test_config_decryption/test_wrong_settings.py	69990
test_config_decryption/test_wrong_settings_zk.py	28050
test_config_decryption/test_zk.py	70740
test_config_decryption/test_zk_secure.py	119170
test_config_hide_in_preprocessed/test.py	65010
test_config_reloader_interval/test.py	115000
test_config_substitutions/test.py	111480
test_config_xml_full/test.py	68300
test_config_xml_main/test.py	33210
test_config_xml_yaml_mix/test.py	86900
test_config_yaml_full/test.py	30750
test_config_yaml_main/test.py	52270
test_config_yaml_merge_keys/test.py	27460
test_consistant_parts_after_move_partition/test.py	138920
test_consistent_parts_after_clone_replica/test.py	152180
test_covered_by_broken_exists/test.py	69410
test_cow_policy/test.py	41490
test_crash_log/test.py	72180
test_create_query_constraints/test.py	38720
test_create_user_and_login/test.py	59530
test_cross_replication/test.py	99270
test_custom_dashboards/test.py	68050
test_custom_settings/test.py	26880
test_database_backup/test.py	258289
test_database_delta/test.py	1416900
test_database_disk/test.py	54600
test_database_disk_setting/test.py	167190
test_database_glue/test.py	132490
test_database_hms/test.py	213430
test_database_iceberg/test.py	236210
test_database_iceberg_lakekeeper_catalog/test.py	165220
test_database_iceberg_nessie_catalog/test.py	154390
test_database_replicated_settings/test.py	1841876
test_ddl_alter_query/test.py	97880
test_ddl_config_hostname/test.py	90630
test_ddl_on_cluster_stop_waiting_for_offline_hosts/test.py	210770
test_ddl_worker_non_leader/test.py	90140
test_ddl_worker_replicas/test.py	349230
test_ddl_worker_with_loopback_hosts/test.py	75140
test_deduplicated_attached_part_rename/test.py	71580
test_default_compression_codec/test.py	205699
test_default_compression_in_mergetree_settings/test.py	61330
test_default_database_on_cluster/test.py	72780
test_default_role/test.py	44000
test_delayed_remote_source/test.py	31830
test_delayed_replica_failover/test.py	154380
test_detached_parts_metrics/test.py	55570
test_dictionaries_access/test.py	50310
test_dictionaries_all_layouts_separate_sources/test_clickhouse_local.py	808869
test_dictionaries_all_layouts_separate_sources/test_clickhouse_remote.py	784070
test_dictionaries_all_layouts_separate_sources/test_executable_cache.py	227870
test_dictionaries_all_layouts_separate_sources/test_executable_hashed.py	275579
test_dictionaries_all_layouts_separate_sources/test_file.py	388510
test_dictionaries_all_layouts_separate_sources/test_http.py	757710
test_dictionaries_all_layouts_separate_sources/test_https.py	796520
test_dictionaries_all_layouts_separate_sources/test_mongo.py	1242020
test_dictionaries_all_layouts_separate_sources/test_mongo_uri.py	350500
test_dictionaries_all_layouts_separate_sources/test_mysql.py	801240
test_dictionaries_config_reload/test.py	152470
test_dictionaries_ddl/test.py	444480
test_dictionaries_dependency/test.py	697880
test_dictionaries_dependency_xml/test.py	171030
test_dictionaries_mysql/test.py	303530
test_dictionaries_null_value/test.py	30010
test_dictionaries_postgresql/test.py	174910
test_dictionaries_redis/test.py	799440
test_dictionaries_redis/test_long.py	166490
test_dictionaries_replace/test.py	151430
test_dictionaries_select_all/test.py	56640
test_dictionaries_update_and_reload/test.py	618530
test_dictionaries_wait_for_load/test.py	97980
test_dictionaries_with_invalid_structure/test.py	31070
test_dictionary_allow_read_expired_keys/test_default_reading.py	55069
test_dictionary_allow_read_expired_keys/test_default_string.py	47430
test_dictionary_allow_read_expired_keys/test_dict_get.py	57600
test_dictionary_allow_read_expired_keys/test_dict_get_or_default.py	58350
test_dictionary_asynchronous_metrics/test.py	51469
test_dictionary_custom_settings/test.py	54360
test_dictionary_ddl_on_cluster/test.py	87180
test_disable_insertion_and_mutation/test.py	72540
test_disabled_access_control_improvements/test_row_policy.py	99338
test_disabled_access_control_improvements/test_select_from_system_tables.py	46610
test_disabled_mysql_server/test.py	78640
test_disk_access_storage/test.py	190740
test_disk_checker/test.py	62309
test_disk_configuration/test.py	220240
test_disk_name_virtual_column/test.py	30900
test_disk_over_web_server/test.py	409690
test_disk_types/test.py	39730
test_disks_app_func/test.py	99409
test_disks_app_interactive/test.py	1430
test_disks_app_other_disk_types/test.py	60520
test_distributed_async_insert_for_node_changes/test.py	72250
test_distributed_config/test.py	38340
test_distributed_ddl/test.py	1306159
test_distributed_ddl/test_replicated_alter.py	407800
test_distributed_ddl_on_cross_replication/test.py	83630
test_distributed_ddl_parallel/test.py	347739
test_distributed_ddl_password/test.py	106490
test_distributed_default_database/test.py	37980
test_distributed_directory_monitor_split_batch_on_failure/test.py	400800
test_distributed_format/test.py	59450
test_distributed_insert_backward_compatibility/test.py	157220
test_distributed_inter_server_secret/test.py	194690
test_distributed_load_balancing/test.py	1284580
test_distributed_over_distributed/test.py	27500
test_distributed_respect_user_timeouts/test.py	1371120
test_distributed_storage_configuration/test.py	64480
test_distributed_system_query/test.py	43310
test_distributed_type_object/test.py	43360
test_dns_cache/test.py	962700
test_dot_in_user_name/test.py	28230
test_dotnet_client/test.py	37250
test_drop_if_empty/test.py	93480
test_drop_is_lock_free/test.py	827240
test_drop_no_local_path/test.py	31980
test_drop_replica/test.py	100630
test_drop_replica_with_auxiliary_zookeepers/test.py	81160
test_enable_user_name_access_type/test.py	50330
test_enabling_access_management/test.py	28460
test_encrypted_disk/test.py	242079
test_encrypted_disk_replication/test.py	104840
test_endpoint_macro_substitution/test.py	42420
test_executable_dictionary/test.py	261120
test_executable_table_function/test.py	1345430
test_executable_udf_names_in_system_query_log/test.py	61620
test_executable_user_defined_function/test.py	168750
test_executable_user_defined_functions_config_reload/test.py	138080
test_explain_estimates/test.py	25450
test_external_cluster/test.py	70060
test_external_http_authenticator/test.py	40440
test_extreme_deduplication/test.py	92330
test_failed_async_inserts/test.py	79430
test_fetch_memory_usage/test.py	112450
test_fetch_partition_from_auxiliary_zookeeper/test.py	81239
test_fetch_partition_should_reset_mutation/test.py	148500
test_fetch_partition_with_outdated_parts/test.py	78270
test_file_cluster/test.py	100180
test_file_schema_inference_cache/test.py	157230
test_filesystem_cache/test.py	396550
test_filesystem_cache_uninitialized/test.py	65980
test_filesystem_layout/test.py	75950
test_fix_metadata_version/test.py	68580
test_force_deduplication/test.py	78680
test_force_drop_table/test.py	88070
test_force_restore_data_flag_for_keeper_dataloss/test.py	94400
test_format_avro_confluent/test.py	79220
test_format_cannot_allocate_thread/test.py	40790
test_format_schema_on_server/test.py	38789
test_format_schema_source/test.py	121300
test_freeze_table/test.py	27740
test_geoparquet/test.py	29220
test_git_import/test.py	41440
test_global_overcommit_tracker/test.py	32890
test_globs_in_filepath/test.py	330089
test_grant_and_revoke/test_with_table_engine_grant.py	202819
test_grant_and_revoke/test_without_table_engine_grant.py	29890
test_graphite_merge_tree/test.py	100050
test_graphite_merge_tree_typed/test.py	108889
test_groupBitmapAnd_on_distributed/test.py	62230
test_groupBitmapAnd_on_distributed/test_groupBitmapAndState_on_distributed_table.py	62220
test_group_array_element_size/test.py	159970
test_grpc_protocol/test.py	66060
test_grpc_protocol_ssl/test.py	40040
test_hedged_requests/test.py	526910
test_hedged_requests_parallel/test.py	187640
test_host_regexp_hosts_file_resolution/test.py	35540
test_host_regexp_multiple_ptr_records/test.py	193369
test_hot_reload_storage_policy/test.py	72760
test_http_and_readonly/test.py	20730
test_http_connection_drain_before_reuse/test.py	58950
test_http_failover/test.py	653110
test_http_handlers_config/test.py	335040
test_http_native/test.py	21400
test_https_replication/test.py	147390
test_https_replication/test_change_ip.py	107250
test_https_s3_table_function_with_http_proxy_no_tunneling/test.py	173200
test_inherit_multiple_profiles/test.py	59880
test_input_format_parallel_parsing_memory_tracking/test.py	23040
test_insert_distributed_async_extra_dirs/test.py	43710
test_insert_distributed_async_send/test.py	320660
test_insert_distributed_load_balancing/test.py	90730
test_insert_into_distributed/test.py	148730
test_insert_into_distributed_sync_async/test.py	131320
test_insert_into_distributed_through_materialized_view/test.py	64810
test_insert_over_http_query_log/test.py	111199
test_inserts_with_keeper_retries/test.py	388100
test_intersecting_parts/test.py	51310
test_interserver_dns_retires/test.py	41530
test_jbod_balancer/test.py	254740
test_jbod_ha/test.py	146230
test_jbod_load_balancing/test.py	39060
test_jdbc_bridge/test.py	72410
test_jemalloc_global_profiler/test.py	55610
test_jemalloc_percpu_arena/test.py	8960
test_join_set_family_s3/test.py	109460
test_kafka_bad_messages/test.py	901420
test_kafka_bad_messages/test_1.py	124289
test_keeper_and_access_storage/test.py	20040
test_keeper_auth/test.py	225400
test_keeper_availability_zone/test.py	46390
test_keeper_back_to_back/test.py	273249
test_keeper_block_acl/test.py	183160
test_keeper_broken_logs/test.py	128790
test_keeper_client/test.py	34849
test_keeper_compression/test_with_compression.py	18600
test_keeper_compression/test_without_compression.py	20060
test_keeper_disks/test.py	242550
test_keeper_dynamic_log_level/test.py	31830
test_keeper_feature_flags_config/test.py	123530
test_keeper_force_recovery/test.py	275740
test_keeper_force_recovery_single_node/test.py	168120
test_keeper_four_word_command/test.py	72850
test_keeper_four_word_command/test_allow_list.py	26030
test_keeper_http_control/test.py	62810
test_keeper_incorrect_config/test.py	378380
test_keeper_internal_secure/test.py	409820
test_keeper_invalid_digest/test.py	123480
test_keeper_ipv4_fallback/test.py	42050
test_keeper_map/test.py	390730
test_keeper_map_retries/test.py	101610
test_keeper_memory_soft_limit/test.py	292220
test_keeper_mntr_data_size/test.py	66770
test_keeper_mntr_pressure/test.py	186840
test_keeper_multinode_simple/test.py	116020
test_keeper_nodes_add/test.py	184350
test_keeper_nodes_move/test.py	169250
test_keeper_nodes_remove/test.py	240380
test_keeper_password/test.py	350360
test_keeper_path_acl/test.py	20060
test_keeper_persistent_log/test.py	84840
test_keeper_persistent_log_multinode/test.py	77680
test_keeper_profiler/test.py	60540
test_keeper_reconfig_add/test.py	125720
test_keeper_reconfig_remove/test.py	102370
test_keeper_reconfig_remove_many/test.py	172450
test_keeper_reconfig_replace_leader/test.py	149560
test_keeper_reconfig_replace_leader_in_one_command/test.py	189570
test_keeper_restore_from_snapshot/test.py	50350
test_keeper_restore_from_snapshot/test_disk_s3.py	73580
test_keeper_s3_snapshot/test.py	148400
test_keeper_secure_client/test.py	43820
test_keeper_session/test.py	71690
test_keeper_snapshot_on_exit/test.py	187840
test_keeper_snapshot_small_distance/test.py	212650
test_keeper_snapshots/test.py	166120
test_keeper_snapshots_multinode/test.py	68240
test_keeper_three_nodes_start/test.py	111920
test_keeper_three_nodes_two_alive/test.py	359180
test_keeper_two_nodes_cluster/test.py	890290
test_keeper_watches/test.py	47750
test_keeper_znode_time/test.py	118930
test_keeper_zookeeper_converter/test.py	524189
test_kerberos_auth/test.py	42310
test_lazy_database/test.py	93049
test_ldap_external_user_directory/test.py	83980
test_library_bridge/test.py	486319
test_library_bridge/test_exiled.py	76640
test_lightweight_updates/test.py	263120
test_limit_materialized_view_count/test.py	44350
test_limited_replicated_fetches/test.py	229070
test_log_family_s3/test.py	64700
test_log_levels_update/test.py	34030
test_log_lz4_streaming/test.py	105990
test_log_query_probability/test.py	780720
test_logs_level/test.py	21280
test_lost_part/test.py	631140
test_lost_part_during_startup/test.py	1371900
test_manipulate_statistics/test.py	128260
test_mark_cache_profile_events/test.py	131610
test_mask_sensitive_info/test.py	586890
test_match_process_uid_against_data_owner/test.py	45610
test_materialized_view_restart_server/test.py	52550
test_matview_union_replicated/test.py	78890
test_max_authentication_methods_per_user/test.py	37460
test_max_bytes_ratio_before_external_order_group_by_for_server/test.py	1179630
test_max_rows_to_read_leaf_with_view/test.py	56880
test_max_suspicious_broken_parts/test.py	56160
test_max_suspicious_broken_parts_replicated/test.py	96950
test_memory_limit/test.py	106600
test_memory_limit_observer/test.py	179090
test_memory_profiler_min_max_borders/test.py	123700
test_merge_table_over_distributed/test.py	63240
test_merge_tree_azure_blob_storage/test.py	251300
test_merge_tree_check_part_with_cache/test.py	54880
test_merge_tree_empty_parts/test.py	53370
test_merge_tree_load_marks/test.py	37230
test_merge_tree_load_parts/test.py	243380
test_merge_tree_prewarm_cache/test.py	33230
test_merge_tree_s3/test.py	1150208
test_merge_tree_s3_failover/test.py	177200
test_merge_tree_s3_with_cache/test.py	114309
test_merge_tree_settings_constraints/test.py	26690
test_merges_memory_limit/test.py	137280
test_modify_engine_on_restart/test.py	174640
test_modify_engine_on_restart/test_args.py	64300
test_modify_engine_on_restart/test_mv.py	69020
test_modify_engine_on_restart/test_ordinary.py	320760
test_modify_engine_on_restart/test_storage_policies.py	167160
test_modify_engine_on_restart/test_unusual_path.py	68550
test_modify_engine_on_restart/test_zk_path_exists.py	90110
test_move_partition_to_disk_on_cluster/test.py	47150
test_move_partition_to_volume_async/test.py	131050
test_move_ttl_broken_compatibility/test.py	207760
test_multi_access_storage_role_management/test.py	139720
test_multiple_disks/test.py	1551789
test_mutation_fetch_fallback/test.py	77850
test_mutations_hardlinks/test.py	351490
test_mutations_in_partitions_of_merge_tree/test.py	90219
test_mutations_with_merge_tree/test.py	150330
test_mutations_with_projection/test.py	82120
test_mysql57_database_engine/test.py	427310
test_mysql_database_engine/test.py	481280
test_mysql_protocol/test.py	539270
test_named_collections/test.py	1047160
test_named_collections_encrypted/test.py	149230
test_named_collections_if_exists_on_cluster/test.py	183780
test_non_default_compression/test.py	166720
test_odbc_interaction/test.py	359470
test_old_parts_finally_removed/test.py	195860
test_old_versions/test.py	58410
test_on_cluster_timeouts/test.py	224770
test_optimize_on_insert/test.py	64540
test_os_thread_nice_value/test.py	78079
test_overcommit_tracker/test.py	66240
test_parallel_replicas_all_marks_read/test.py	127900
test_parallel_replicas_custom_key/test.py	137990
test_parallel_replicas_custom_key_failover/test.py	261770
test_parallel_replicas_custom_key_load_balancing/test.py	226648
test_parallel_replicas_distributed_skip_shards/test.py	171290
test_parallel_replicas_failover/test.py	99590
test_parallel_replicas_increase_error_count/test.py	180140
test_parallel_replicas_insert_select/test.py	506889
test_parallel_replicas_invisible_parts/test.py	380870
test_parallel_replicas_no_replicas/test.py	61240
test_parallel_replicas_over_distributed/test.py	359150
test_parallel_replicas_protocol/test.py	82730
test_parallel_replicas_skip_shards/test.py	44720
test_parallel_replicas_snapshot_from_initiator/test.py	99590
test_parquet_page_index/test.py	28470
test_part_log_table/test.py	53090
test_part_uuid/test.py	87420
test_partition/test.py	139780
test_parts_delete_zookeeper/test.py	117000
test_passing_max_partitions_to_read_remotely/test.py	69980
test_password_constraints/test.py	36179
test_peak_memory_usage/test.py	69570
test_permissions_drop_replica/test.py	94080
test_placement_info/test.py	53470
test_plain_rewr_legacy_layout/test.py	60900
test_polymorphic_parts/test.py	384359
test_postgresql_database_engine/test.py	583910
test_postgresql_protocol/test.py	71440
test_postgresql_replica_database_engine/test_0.py	333060
test_postgresql_replica_database_engine/test_1.py	523129
test_postgresql_replica_database_engine/test_2.py	443849
test_postgresql_replica_database_engine/test_3.py	417858
test_postpone_failed_tasks/test.py	584770
test_prefer_global_in_and_join/test.py	88990
test_profile_events_s3/test.py	59280
test_profile_max_sessions_for_user/test.py	71850
test_profile_settings_and_constraints_order/test.py	31570
test_prometheus_endpoint/test.py	30250
test_prometheus_protocols/test_different_table_engines.py	83920
test_prometheus_protocols/test_evaluation.py	57510
test_prometheus_protocols/test_write_read.py	69970
test_quorum_inserts/test.py	292950
test_quorum_inserts_parallel/test.py	107510
test_quota/test.py	184810
test_random_inserts/test.py	320460
test_range_hashed_dictionary_types/test.py	25210
test_read_only_table/test.py	134450
test_read_temporary_tables_on_failure/test.py	46420
test_recompression_ttl/test.py	379140
test_recovery_replica/test.py	192700
test_recovery_time_metric/test.py	75710
test_refreshable_mat_view/test.py	2233369
test_refreshable_mat_view_replicated/test.py	1201389
test_refreshable_mv/test.py	994499
test_refreshable_mv_skip_old_temp_table_ddls/test.py	1557940
test_relative_filepath/test.py	22710
test_reload_auxiliary_zookeepers/test.py	101610
test_reload_certificate/test.py	32950
test_reload_client_certificate/test.py	159400
test_reload_clusters_config/test.py	93820
test_reload_max_table_size_to_drop/test.py	44580
test_reload_query_masking_rules/test.py	33630
test_reload_zookeeper/test.py	120200
test_reloading_settings_from_users_xml/test.py	60370
test_reloading_storage_configuration/test.py	266820
test_remote_blobs_naming/test_backward_compatibility.py	200610
test_remote_function_view/test.py	36300
test_remote_prewhere/test.py	29870
test_remove_stale_moving_parts/test.py	128990
test_rename_column/test.py	314960
test_render_log_file_name_templates/test.py	29870
test_replace_partition/test.py	122520
test_replica_can_become_leader/test.py	97090
test_replica_is_active/test.py	89360
test_replicated_database/test.py	1902879
test_replicated_database/test_settings_recover_lost_replica.py	106250
test_replicated_database_alter_modify_order_by/test.py	47710
test_replicated_database_cluster_groups/test.py	223170
test_replicated_database_recover_digest_mismatch/test.py	193850
test_replicated_detach_table/test.py	60270
test_replicated_engine_arguments/test.py	59520
test_replicated_fetches_bandwidth/test.py	372230
test_replicated_fetches_timeouts/test.py	100459
test_replicated_merge_tree_compatibility/test.py	335400
test_replicated_merge_tree_config/test.py	50140
test_replicated_merge_tree_encrypted_disk/test.py	91120
test_replicated_merge_tree_encryption_codec/test.py	125910
test_replicated_merge_tree_replicated_db_ttl/test.py	63030
test_replicated_merge_tree_s3/test.py	111070
test_replicated_merge_tree_thread_schedule_timeouts/test.py	56630
test_replicated_merge_tree_wait_on_shutdown/test.py	112180
test_replicated_merge_tree_with_auxiliary_zookeepers/test.py	178280
test_replicated_mutations/test.py	585510
test_replicated_parse_zk_metadata/test.py	117620
test_replicated_s3_zero_copy_drop_partition/test.py	86870
test_replicated_table_attach/test.py	205800
test_replicated_user_defined_functions/test.py	254510
test_replicated_users/test.py	388039
test_replicating_constants/test.py	141620
test_replication_credentials/test.py	138900
test_replication_without_zookeeper/test.py	87210
test_restart_server/test.py	43940
test_restart_with_unavailable_azure/test.py	89380
test_restore_db_replica/test.py	1595270
test_restore_external_engines/test.py	323310
test_restore_replica/test.py	488100
test_rocksdb_options/test.py	275410
test_rocksdb_read_only/test.py	70380
test_role/test.py	352610
test_row_policy/test.py	542189
test_runtime_configurable_cache_size/test.py	30740
test_s3_access_headers/test.py	100140
test_s3_aws_sdk_has_slightly_unreliable_behaviour/test.py	2422860
test_s3_cluster/test.py	168350
test_s3_cluster_insert_select/test.py	86070
test_s3_cluster_restart/test.py	259949
test_s3_imds/test_session_token.py	44690
test_s3_imds/test_simple.py	50360
test_s3_low_cardinality_right_border/test.py	56670
test_s3_plain_rewritable/test.py	485830
test_s3_plain_rewritable_rotate_tables/test.py	136970
test_s3_storage_class/test.py	95890
test_s3_storage_conf_new_proxy/test.py	200840
test_s3_storage_conf_proxy/test.py	129050
test_s3_style_link/test.py	42790
test_s3_table_function_with_http_proxy/test.py	302430
test_s3_table_function_with_https_proxy/test.py	319830
test_s3_table_functions/test.py	175400
test_s3_with_https/test.py	44550
test_scheduler/test.py	671590
test_scheduler_cpu/test.py	225459
test_scheduler_cpu_preemptive/test.py	3231110
test_scheduler_query/test.py	277370
test_scram_sha256_password_with_replicated_zookeeper_replicator/test.py	126200
test_search_orphaned_parts/test.py	72570
test_secure_socket/test.py	181610
test_select_access_rights/test_from_system_tables.py	83780
test_select_access_rights/test_main.py	110630
test_send_crash_reports/test.py	40640
test_send_request_to_leader_replica/test.py	117080
test_server_initialization/test.py	44490
test_server_keep_alive/test.py	36110
test_server_overload/test.py	138359
test_server_reload/test.py	163490
test_server_start_and_ip_conversions/test.py	107050
test_server_startup_and_shutdown_logs/test.py	30590
test_session_log/test.py	54460
test_session_settings_table/test.py	197200
test_settings_constraints/test.py	69420
test_settings_constraints_distributed/test.py	103180
test_settings_from_server/test.py	44600
test_settings_profile/test.py	129420
test_settings_randomization/test.py	24150
test_shard_level_const_function/test.py	62510
test_shard_names/test.py	30980
test_sharding_key_from_default_column/test.py	131910
test_shutdown_static_destructor_failure/test.py	37850
test_shutdown_wait_unfinished_queries/test.py	86490
test_sql_user_defined_functions_on_cluster/test.py	81480
test_ssh/test.py	29400
test_ssh/test_options_propagation_enabled.py	29010
test_ssh_keys_authentication/test.py	37980
test_ssl_cert_authentication/test.py	43010
test_startup_scripts/test.py	52650
test_startup_scripts_execution_state/test.py	145090
test_stop_insert_when_disk_close_to_full/test.py	85800
test_storage_azure_blob_storage/test.py	1739730
test_storage_azure_blob_storage/test_check_after_upload.py	66560
test_storage_azure_blob_storage/test_cluster.py	70060
test_storage_delta/test.py	4859398
test_storage_delta/test_imds.py	86850
test_storage_delta_disks/test.py	159000
test_storage_dict/test.py	36680
test_storage_hudi/test.py	372750
test_storage_iceberg_disks/test.py	166998
test_storage_iceberg_no_spark/test_graceful_error_not_configured_iceberg_metadata_log.py	56150
test_storage_iceberg_no_spark/test_time_travel_bug_fix_validation.py	60350
test_storage_iceberg_no_spark/test_writes_create_table_bugs.py	80700
test_storage_iceberg_no_spark/test_writes_multiple_files.py	93710
test_storage_iceberg_no_spark/test_writes_nullable_bugs2.py	81780
test_storage_iceberg_no_spark/test_writes_statistics_by_minmax_pruning.py	316160
test_storage_iceberg_no_spark/test_writes_with_compression_metadata.py	76750
test_storage_iceberg_schema_evolution/test_array_evolved_nested.py	527070
test_storage_iceberg_schema_evolution/test_array_evolved_with_struct.py	198600
test_storage_iceberg_schema_evolution/test_correct_column_mapper_is_chosen.py	102110
test_storage_iceberg_schema_evolution/test_evolved_schema_complex.py	189770
test_storage_iceberg_schema_evolution/test_evolved_schema_simple.py	819490
test_storage_iceberg_schema_evolution/test_full_drop.py	84160
test_storage_iceberg_schema_evolution/test_map_evolved_nested.py	130330
test_storage_iceberg_schema_evolution/test_tuple_evolved_nested.py	296420
test_storage_iceberg_with_spark/test_bucket_partition_pruning.py	129850
test_storage_iceberg_with_spark/test_cluster_table_function.py	179100
test_storage_iceberg_with_spark/test_cluster_table_function_with_partition_pruning.py	72980
test_storage_iceberg_with_spark/test_compressed_metadata.py	101460
test_storage_iceberg_with_spark/test_delete_files.py	210040
test_storage_iceberg_with_spark/test_explanation.py	140709
test_storage_iceberg_with_spark/test_explicit_metadata_file.py	216750
test_storage_iceberg_with_spark/test_filesystem_cache.py	107320
test_storage_iceberg_with_spark/test_iceberg_snapshot_reads.py	228910
test_storage_iceberg_with_spark/test_metadata_cache.py	106080
test_storage_iceberg_with_spark/test_metadata_file_format_with_uuid.py	345910
test_storage_iceberg_with_spark/test_metadata_file_selection.py	341520
test_storage_iceberg_with_spark/test_metadata_file_selection_from_version_hint.py	177580
test_storage_iceberg_with_spark/test_minmax_pruning.py	539420
test_storage_iceberg_with_spark/test_minmax_pruning_for_arrays_and_maps_subfields_disabled.py	78170
test_storage_iceberg_with_spark/test_minmax_pruning_with_null.py	145360
test_storage_iceberg_with_spark/test_multiple_iceberg_file.py	116220
test_storage_iceberg_with_spark/test_optimize.py	141869
test_storage_iceberg_with_spark/test_partition_by.py	103870
test_storage_iceberg_with_spark/test_partition_pruning.py	395690
test_storage_iceberg_with_spark/test_position_deletes.py	422420
test_storage_iceberg_with_spark/test_pruning_nullable_bug.py	96530
test_storage_iceberg_with_spark/test_relevant_iceberg_schema_chosen.py	99390
test_storage_iceberg_with_spark/test_restart_broken_s3.py	98470
test_storage_iceberg_with_spark/test_schema_evolution_with_time_travel.py	238639
test_storage_iceberg_with_spark/test_schema_inference.py	255708
test_storage_iceberg_with_spark/test_single_iceberg_file.py	119240
test_storage_iceberg_with_spark/test_system_iceberg_metadata.py	311250
test_storage_iceberg_with_spark/test_types.py	96190
test_storage_iceberg_with_spark/test_writes.py	171790
test_storage_iceberg_with_spark/test_writes_complex_type.py	131580
test_storage_iceberg_with_spark/test_writes_create_partitioned_table.py	227959
test_storage_iceberg_with_spark/test_writes_create_table.py	114950
test_storage_iceberg_with_spark/test_writes_create_version_hint.py	82680
test_storage_iceberg_with_spark/test_writes_different_path_format_error.py	72150
test_storage_iceberg_with_spark/test_writes_drop_table.py	69269
test_storage_iceberg_with_spark/test_writes_field_partitioning.py	78310
test_storage_iceberg_with_spark/test_writes_from_zero.py	102680
test_storage_iceberg_with_spark/test_writes_mutate_delete.py	252650
test_storage_iceberg_with_spark/test_writes_mutate_update.py	242190
test_storage_iceberg_with_spark/test_writes_schema_evolution.py	143300
test_storage_iceberg_with_spark/test_writes_with_partitioned_table.py	126540
test_storage_kafka/test_batch_fast.py	3215888
test_storage_kafka/test_batch_slow_0.py	429280
test_storage_kafka/test_batch_slow_1.py	1013440
test_storage_kafka/test_batch_slow_2.py	863930
test_storage_kafka/test_batch_slow_4.py	727940
test_storage_kafka/test_batch_slow_5.py	691820
test_storage_kafka/test_batch_slow_6.py	706150
test_storage_kafka/test_intent_sizes.py	138280
test_storage_kafka/test_produce_http_interface.py	205210
test_storage_kafka/test_zookeeper_locks.py	137110
test_storage_kafka_sasl/test.py	104100
test_storage_kerberized_kafka/test.py	495229
test_storage_mongodb/test.py	338240
test_storage_mysql/test.py	277970
test_storage_nats/test_nats_core.py	956410
test_storage_nats/test_nats_jet_stream.py	1488770
test_storage_numbers/test.py	106090
test_storage_policies/test.py	150920
test_storage_postgresql/test.py	433909
test_storage_rabbitmq/test.py	3204987
test_storage_rabbitmq/test_failed_connection.py	381630
test_storage_redis/test.py	107770
test_storage_s3/test.py	2073910
test_storage_s3/test_invalid_env_credentials.py	122790
test_storage_s3_intelligent_tier/test.py	45670
test_storage_s3_queue/test_0.py	1184400
test_storage_s3_queue/test_1.py	1090270
test_storage_s3_queue/test_2.py	1087460
test_storage_s3_queue/test_3.py	824209
test_storage_s3_queue/test_4.py	553579
test_storage_s3_queue/test_5.py	4471579
test_storage_s3_queue/test_parallel_inserts.py	120119
test_storage_url/test.py	52370
test_storage_url_http_headers/test.py	37220
test_storage_url_with_proxy/test.py	82200
test_store_cleanup/test.py	289160
test_structured_logging_json/test.py	35280
test_sync_replica_on_cluster/test.py	76930
test_system_clusters_actual_information/test.py	540040
test_system_ddl_worker_queue/test.py	193340
test_system_detached_tables/test.py	174590
test_system_flush_logs/test.py	135150
test_system_logs/test_system_logs.py	561279
test_system_logs_comment/test.py	34390
test_system_logs_hostname/test_replicated.py	73500
test_system_logs_recreate/test.py	270190
test_system_merges/test.py	450350
test_system_metrics/test.py	234650
test_system_queries/test.py	79830
test_system_reload_async_metrics/test.py	16140
test_system_reload_async_metrics/test_async_metrics_invalid_settings.py	8110
test_system_start_stop_listen/test.py	150160
test_table_db_num_limit/test.py	177360
test_table_function_mongodb/test.py	127270
test_table_function_redis/test.py	72310
test_table_functions_access_rights/test.py	38659
test_tcp_handler_connection_limits/test.py	44719
test_tcp_handler_http_responses/test_case.py	246390
test_tcp_handler_interserver_listen_host/test_case.py	53670
test_temporary_data/test.py	76820
test_temporary_data_in_cache/test.py	83360
test_text_log_level/test.py	27260
test_threadpool_readers/test.py	102940
test_throttling/test.py	1706819
test_timezone_config/test.py	32650
test_tlsv1_3/test.py	28110
test_tmp_policy/test.py	171440
test_trace_collector_serverwide/test.py	45279
test_trace_log_build_id/test.py	163610
test_transactions/test.py	97060
test_transposed_metric_log/test.py	535470
test_truncate_database/test_distributed.py	122550
test_truncate_database/test_replicated.py	81820
test_ttl_move/test.py	3464388
test_ttl_multilevel_group_by/test.py	98410
test_ttl_replicated/test.py	783660
test_unambiguous_alter_commands/test.py	24490
test_undrop_query/test.py	113550
test_union_header/test.py	59160
test_unknown_column_dist_table_with_alias/test.py	32860
test_user_defined_object_persistence/test.py	77600
test_user_directories/test.py	174390
test_user_grants_from_config/test.py	33700
test_user_ip_restrictions/test.py	41170
test_user_valid_until/test.py	114360
test_user_zero_database_access/test_user_zero_database_access.py	44550
test_userspace_page_cache/test.py	166740
test_userspace_page_cache/test_incorrect_limits.py	40590
test_validate_only_initial_alter_query/test_replicated_database.py	68730
test_validate_threadpool_writer_pool_size/test.py	7680
test_version_update/test.py	211470
test_version_update_after_mutation/test.py	447890
test_warning_broken_tables/test.py	156320
test_wrong_db_or_table_name/test.py	48760
test_ytsaurus/test_dictionaries.py	380599
test_ytsaurus/test_tables.py	638118
test_zero_copy_expand_macros/test.py	63650
test_zookeeper_config/test.py	104980
test_zookeeper_config/test_password.py	194510
test_zookeeper_config/test_secure.py	175190
test_zookeeper_config_load_balancing/test.py	141529
test_zookeeper_connection_log/test.py	99460
test_zookeeper_fallback_session/test.py	82600
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
