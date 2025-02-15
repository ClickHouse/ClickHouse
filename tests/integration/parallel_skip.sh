# Parallel tests skip list is currently generated manually. All tests except those in parallel_skip.txt will run in parallel.
# Current list is generated with following commands
# 1. Put non-parallel test into the array
# ending with / means the directory, without trailing slash acts as glob
NON_PARALLEL_TESTS=(
  test_atomic_drop_table/
  test_attach_without_fetching/
  test_broken_part_during_merge/
  test_cleanup_dir_after_bad_zk_conn/
  test_consistent_parts_after_clone_replica/
  test_cross_replication/
  test_ddl_worker_non_leader/
  test_delayed_replica_failover/
  test_dictionary_allow_read_expired_keys/
  test_disabled_mysql_server/
  test_distributed_ddl  # all test_distributed_ddl* tests
  test_distributed_respect_user_timeouts/
  test_drop_replica  # all test_drop_replica* tests
  test_hedged_requests/
  test_hedged_requests_parallel/
  test_https_replication/
  test_insert_into_distributed  # all test_insert_into_distributed*
  test_keeper_multinode_simple/
  test_limited_replicated_fetches/
  test_materialized_mysql_database/
  test_parts_delete_zookeeper/
  test_polymorphic_parts/
  test_quorum_inserts_parallel/
  test_random_inserts/
  test_reload_clusters_config/
  test_replace_partition/
  test_replicated_database  # all test_replicated_database*
  test_replicated_fetches_timeouts/
  test_storage_kafka/
  test_storage_kerberized_kafka/
  test_system_clusters_actual_information/
  test_system_metrics/
  test_system_replicated_fetches/
)
# 2. Filter known tests that are currently not run in parallel
printf "%s\n" "${NON_PARALLEL_TESTS[@]}" | jq -R -n '[inputs] | .' > parallel_skip.json
