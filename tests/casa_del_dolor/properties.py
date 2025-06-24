import xml.etree.ElementTree as ET
import tempfile
import multiprocessing
import random
import string
import typing

from integration.helpers.cluster import ClickHouseCluster


def threshold_generator(always_on_prob, always_off_prob, min_val, max_val):
    def gen():
        tmp = random.random()
        if tmp <= always_on_prob:
            return min_val
        if tmp <= always_on_prob + always_off_prob:
            return max_val

        if isinstance(min_val, int) and isinstance(max_val, int):
            return random.randint(min_val, max_val)
        return random.uniform(min_val, max_val)

    return gen


def file_size_value(max_val: int):
    def gen():
        return str(threshold_generator(0.2, 0.2, 1, max_val)()) + random.choice(
            ["ki", "ki", "Mi", "Gi"]  # Increased probability
        )

    return gen


true_false_lambda = lambda: random.randint(0, 1)
threads_lambda = lambda: random.randint(0, multiprocessing.cpu_count())
no_zero_threads_lambda = lambda: random.randint(1, multiprocessing.cpu_count())


possible_properties = {
    "aggregate_function_group_array_action_when_limit_is_reached": lambda: random.choice(
        ["throw", "discard"]
    ),
    "aggregate_function_group_array_max_element_size": threshold_generator(
        0.2, 0.2, 0, 10000
    ),
    "allow_use_jemalloc_memory": true_false_lambda,
    "async_insert_queue_flush_on_shutdown": true_false_lambda,
    "async_insert_threads": threads_lambda,
    "async_load_databases": true_false_lambda,
    "async_load_system_database": true_false_lambda,
    "asynchronous_heavy_metrics_update_period_s": threshold_generator(0.2, 0.2, 0, 60),
    "asynchronous_metrics_enable_heavy_metrics": true_false_lambda,
    "asynchronous_metrics_update_period_s": threshold_generator(0.2, 0.2, 0, 30),
    "background_buffer_flush_schedule_pool_size": threads_lambda,
    "background_common_pool_size": no_zero_threads_lambda,
    "background_distributed_schedule_pool_size": no_zero_threads_lambda,
    "background_fetches_pool_size": no_zero_threads_lambda,
    # "background_merges_mutations_concurrency_ratio": threshold_generator(
    #    0.2, 0.2, 0.0, 3.0
    # ),
    "background_merges_mutations_scheduling_policy": lambda: random.choice(
        ["round_robin", "shortest_task_first"]
    ),
    "background_message_broker_schedule_pool_size": no_zero_threads_lambda,
    "background_move_pool_size": no_zero_threads_lambda,
    # "background_pool_size": threads_lambda, has to be in a certain range
    "background_schedule_pool_size": no_zero_threads_lambda,
    "backup_threads": no_zero_threads_lambda,
    "backups_io_thread_pool_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "bcrypt_workfactor": threshold_generator(0.2, 0.2, 0, 20),
    "cache_size_to_ram_max_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    # "cannot_allocate_thread_fault_injection_probability": threshold_generator(0.2, 0.2, 0.0, 1.0), the server may not start
    "cgroup_memory_watcher_hard_limit_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "cgroup_memory_watcher_soft_limit_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "compiled_expression_cache_elements_size": threshold_generator(0.2, 0.2, 0, 10000),
    "compiled_expression_cache_size": threshold_generator(0.2, 0.2, 0, 10000),
    "concurrent_threads_scheduler": lambda: random.choice(
        ["round_robin", "fair_round_robin"]
    ),
    "concurrent_threads_soft_limit_num": threads_lambda,
    "concurrent_threads_soft_limit_ratio_to_cores": threads_lambda,
    "database_catalog_drop_table_concurrency": threads_lambda,
    "database_replicated_allow_detach_permanently": true_false_lambda,
    "dictionaries_lazy_load": true_false_lambda,
    "disable_insertion_and_mutation": true_false_lambda,
    "disable_internal_dns_cache": true_false_lambda,
    "display_secrets_in_show_and_select": true_false_lambda,
    "distributed_cache_keep_up_free_connections_ratio": threshold_generator(
        0.2, 0.2, 0.0, 1.0
    ),
    "enable_azure_sdk_logging": true_false_lambda,
    "format_alter_operations_with_parentheses": true_false_lambda,
    "ignore_empty_sql_security_in_create_view_query": true_false_lambda,
    "index_mark_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "index_mark_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "index_mark_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "index_uncompressed_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "index_uncompressed_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "index_uncompressed_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "io_thread_pool_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "keeper_multiread_batch_size": threshold_generator(0.2, 0.2, 1, 1000),
    "load_marks_threadpool_pool_size": threads_lambda,
    "load_marks_threadpool_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "mark_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "mark_cache_prewarm_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "mark_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "mark_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "max_active_parts_loading_thread_pool_size": threads_lambda,
    "max_backup_bandwidth_for_server": threshold_generator(0.2, 0.2, 0, 100000),
    "max_backups_io_thread_pool_free_size": threshold_generator(0.2, 0.2, 0, 1000),
    "max_backups_io_thread_pool_size": threads_lambda,
    "max_build_vector_similarity_index_thread_pool_size": threads_lambda,
    "max_database_num_to_throw": threshold_generator(0.2, 0.2, 0, 10),
    "max_database_replicated_create_table_thread_pool_size": threads_lambda,
    "max_dictionary_num_to_throw": threshold_generator(0.2, 0.2, 0, 10),
    "max_entries_for_hash_table_stats": threshold_generator(0.2, 0.2, 0, 10000),
    "max_fetch_partition_thread_pool_size": threads_lambda,
    "max_io_thread_pool_free_size": threshold_generator(0.2, 0.2, 0, 1000),
    "max_io_thread_pool_size": threads_lambda,
    "max_local_read_bandwidth_for_server": threshold_generator(0.2, 0.2, 0, 100000),
    "max_local_write_bandwidth_for_server": threshold_generator(0.2, 0.2, 0, 100000),
    "max_materialized_views_count_for_table": threshold_generator(0.2, 0.2, 0, 8),
    "max_merges_bandwidth_for_server": threshold_generator(0.2, 0.2, 0, 100000),
    "max_mutations_bandwidth_for_server": threshold_generator(0.2, 0.2, 0, 100000),
    "max_open_files": threshold_generator(0.2, 0.2, 0, 100),
    "max_outdated_parts_loading_thread_pool_size": threads_lambda,
    "max_partition_size_to_drop": threshold_generator(0.2, 0.2, 0, 100000),
    "max_parts_cleaning_thread_pool_size": threads_lambda,
    "max_prefixes_deserialization_thread_pool_free_size": threshold_generator(
        0.2, 0.2, 0, 1000
    ),
    "max_prefixes_deserialization_thread_pool_size": threads_lambda,
    "max_remote_read_network_bandwidth_for_server": threshold_generator(
        0.2, 0.2, 0, 1000
    ),
    "max_remote_write_network_bandwidth_for_server": threshold_generator(
        0.2, 0.2, 0, 1000
    ),
    "max_replicated_fetches_network_bandwidth_for_server": threshold_generator(
        0.2, 0.2, 0, 1000
    ),
    "max_replicated_sends_network_bandwidth_for_server": threshold_generator(
        0.2, 0.2, 0, 1000
    ),
    # "max_server_memory_usage": threshold_generator(0.2, 0.2, 0, 10),
    "max_server_memory_usage_to_ram_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "max_table_num_to_throw": threshold_generator(0.2, 0.2, 0, 10),
    "max_temporary_data_on_disk_size": threshold_generator(0.2, 0.2, 0, 1000),
    "max_thread_pool_free_size": threshold_generator(0.2, 0.2, 0, 1000),
    "max_thread_pool_size": threshold_generator(0.2, 0.2, 700, 10000),
    "max_unexpected_parts_loading_thread_pool_size": threads_lambda,
    "max_waiting_queries": threshold_generator(0.2, 0.2, 0, 100),
    "memory_worker_correct_memory_tracker": true_false_lambda,
    "memory_worker_use_cgroup": true_false_lambda,
    "merges_mutations_memory_usage_soft_limit": threshold_generator(0.2, 0.2, 0, 1000),
    "merges_mutations_memory_usage_to_ram_ratio": threshold_generator(
        0.2, 0.2, 0.0, 1.0
    ),
    "mlock_executable": true_false_lambda,
    "mmap_cache_size": threshold_generator(0.2, 0.2, 0, 2000),
    "page_cache_free_memory_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "page_cache_history_window_ms": threshold_generator(0.2, 0.2, 0, 1000),
    "page_cache_max_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "page_cache_min_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "page_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "page_cache_shards": threshold_generator(0.2, 0.2, 0, 10),
    "page_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "parts_kill_delay_period": threshold_generator(0.2, 0.2, 0, 60),
    "parts_kill_delay_period_random_add": threshold_generator(0.2, 0.2, 0, 100),
    "parts_killer_pool_size": threads_lambda,  # Cloud setting
    "primary_index_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "primary_index_cache_prewarm_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "primary_index_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "primary_index_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "prefetch_threadpool_pool_size": threshold_generator(0.2, 0.2, 0, 1000),
    "prefetch_threadpool_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "prefixes_deserialization_thread_pool_thread_pool_queue_size": threshold_generator(
        0.2, 0.2, 0, 1000
    ),
    "process_query_plan_packet": true_false_lambda,
    "query_cache": {
        "max_entries": threshold_generator(0.2, 0.2, 0, 1024),
        "max_entry_size_in_bytes": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
        "max_entry_size_in_rows": threshold_generator(0.2, 0.2, 0, 10000),
        "max_size_in_bytes": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    },
    "query_condition_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "query_condition_cache_size": threshold_generator(0.2, 0.2, 0, 104857600),
    "query_condition_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "remap_executable": true_false_lambda,
    "restore_threads": no_zero_threads_lambda,
    "shutdown_wait_backups_and_restores": true_false_lambda,
    "shutdown_wait_unfinished_queries": true_false_lambda,
    "startup_mv_delay_ms": threshold_generator(0.2, 0.2, 0, 1000),
    "storage_metadata_write_full_object_key": true_false_lambda,
    "storage_shared_set_join_use_inner_uuid": true_false_lambda,
    "tables_loader_background_pool_size": threads_lambda,
    "tables_loader_foreground_pool_size": threads_lambda,
    "thread_pool_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "threadpool_writer_pool_size": threshold_generator(0.2, 0.2, 0, 200),
    "threadpool_writer_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "throw_on_unknown_workload": true_false_lambda,
    "transaction_log": {
        "fault_probability_after_commit": threshold_generator(0.2, 0.2, 0.0, 1.0),
        "fault_probability_before_commit": threshold_generator(0.2, 0.2, 0.0, 1.0),
    },
    "uncompressed_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "uncompressed_cache_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "uncompressed_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "use_minimalistic_part_header_in_zookeeper": true_false_lambda,
    "validate_tcp_client_information": true_false_lambda,
    "vector_similarity_index_cache_max_entries": threshold_generator(0.2, 0.2, 0, 1000),
    "vector_similarity_index_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "vector_similarity_index_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "vector_similarity_index_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "wait_dictionaries_load_at_startup": true_false_lambda,
    "zookeeper": {
        "use_compression": true_false_lambda,
        "zookeeper_load_balancing": lambda: random.choice(
            [
                "random",
                "in_order",
                "nearest_hostname",
                "hostname_levenshtein_distance",
                "first_or_random",
                "round_robin",
            ]
        ),
    },
}

distributed_ddl_properties = {
    "cleanup_delay_period": threshold_generator(0.2, 0.2, 0, 60),
    "max_tasks_in_queue": threshold_generator(0.2, 0.2, 0, 1000),
    "pool_size": no_zero_threads_lambda,
    "task_max_lifetime": threshold_generator(0.2, 0.2, 0, 60),
}

object_storages_properties = {
    "local": {},
    "s3": {
        "metadata_keep_free_space_bytes": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "min_bytes_for_seek": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
        "object_metadata_cache_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "s3_check_objects_after_upload": true_false_lambda,
        "s3_max_inflight_parts_for_one_file": threshold_generator(0.2, 0.2, 0, 16),
        "s3_max_get_burst": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_get_rps": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_put_burst": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_put_rps": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_single_part_upload_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        # "server_side_encryption_customer_key_base64": true_false_lambda, not working well
        "skip_access_check": true_false_lambda,
        "support_batch_delete": true_false_lambda,
        "thread_pool_size": threads_lambda,
        "use_insecure_imds_request": true_false_lambda,
    },
    "azure": {
        "max_single_download_retries": threshold_generator(0.2, 0.2, 0, 16),
        "max_single_part_upload_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "max_single_read_retries": threshold_generator(0.2, 0.2, 0, 16),
        "metadata_keep_free_space_bytes": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "min_bytes_for_seek": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
        "min_upload_part_size": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
        "skip_access_check": true_false_lambda,
        "thread_pool_size": threads_lambda,
        "use_native_copy": true_false_lambda,
    },
    "web": {},
}


cache_storage_properties = {
    "allow_dynamic_cache_resize": true_false_lambda,
    "background_download_max_file_segment_size": threshold_generator(
        0.2, 0.2, 0, 10 * 1024 * 1024
    ),
    "background_download_queue_size_limit": threshold_generator(0.2, 0.2, 0, 128),
    "background_download_threads": threads_lambda,
    "boundary_alignment": threshold_generator(0.2, 0.2, 0, 128),
    "cache_hits_threshold": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    "cache_on_write_operations": true_false_lambda,
    "cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "enable_bypass_cache_with_threshold": true_false_lambda,
    "enable_filesystem_query_cache_limit": true_false_lambda,
    "keep_free_space_elements_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "keep_free_space_remove_batch": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    "keep_free_space_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "load_metadata_asynchronously": true_false_lambda,
    "load_metadata_threads": threads_lambda,
    "max_elements": threshold_generator(0.2, 0.2, 2, 10000000),
    "max_file_segment_size": file_size_value(100),
    # "max_size_ratio_to_total_space": threshold_generator(0.2, 0.2, 0.0, 1.0), cannot be specified with `max_size` at the same time
    "slru_size_ratio": threshold_generator(0.2, 0.2, 0.01, 0.99),
    "write_cache_per_user_id_directory": true_false_lambda,
}


policy_properties = {
    "load_balancing": lambda: random.choice(["round_robin", "least_used"]),
    "max_data_part_size_bytes": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    "move_factor": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "perform_ttl_move_on_insert": true_false_lambda,
    "prefer_not_to_merge": true_false_lambda,
}


all_disks_properties = {
    "keep_free_space_bytes": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
}


Parameter = typing.Callable[[], int | float]


def sample_from_dict(d: dict[str, Parameter], sample: int) -> dict[str, Parameter]:
    items = random.sample(list(d.items()), sample)
    return dict(items)


def add_settings_from_dict(d: dict[str, Parameter], xml_element: ET.Element):
    selected_props = sample_from_dict(d, random.randint(1, len(d)))
    for setting, generator in selected_props.items():
        new_element = ET.SubElement(xml_element, setting)
        new_element.text = str(generator())


def apply_properties_recursively(next_root: ET.Element, next_properties: dict):
    is_modified = False
    selected_props = sample_from_dict(
        next_properties, random.randint(1, len(next_properties))
    )
    for setting, next_child in selected_props.items():
        if next_root.find(setting) is None:
            is_modified = True
            new_element = ET.SubElement(next_root, setting)
            if isinstance(next_child, dict):
                apply_properties_recursively(new_element, next_child)
            else:
                new_element.text = str(next_child())
    return is_modified


def generate_xml_safe_string(length: int = 10) -> str:
    """
    Generate a random string that is safe to use as XML value content.

    Args:
        length (int): Desired length of the string (default: 10)

    Returns:
        str: Random string containing only XML-safe characters
    """
    # XML 1.0 valid characters (excluding control chars except tab, LF, CR)
    xml_safe_chars = (
        "\t\n\r"  # allowed control chars
        + string.ascii_letters
        + string.digits
        + " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"  # punctuation
        + "\u0020\ud7ff\ue000-\ufffd"  # other valid Unicode ranges
    )
    # For Python 3, we need to handle the Unicode ranges properly
    # Create a list of valid characters
    valid_chars = []
    # Add basic ASCII characters
    valid_chars.extend(c for c in xml_safe_chars if ord(c) < 0xD800)
    # Add higher Unicode characters (avoid surrogates)
    valid_chars.extend(chr(c) for c in range(0xE000, 0xFFFD + 1))
    # Generate the random string
    return "".join(random.choice(valid_chars) for _ in range(length))


def add_single_cluster(
    existing_nodes: list[str],
    next_cluster: ET.Element,
):
    number_elements = (
        1 if random.randint(1, 4) == 1 else random.randint(1, len(existing_nodes))
    )
    next_shard_xml = None
    single_shard = random.randint(1, 100) <= 20

    input_nodes = list(existing_nodes)  # Do deep copy
    random.shuffle(input_nodes)
    # Add secret
    if random.randint(1, 100) <= 30:
        secret_xml = ET.SubElement(next_cluster, "secret")
        secret_xml.text = generate_xml_safe_string(random.randint(1, 128))
    # Add allow_distributed_ddl_queries
    if random.randint(1, 100) <= 16:
        allow_ddl_xml = ET.SubElement(next_cluster, "allow_distributed_ddl_queries")
        allow_ddl_xml.text = "false" if random.randint(1, 4) <= 3 else "true"

    for j in range(0, number_elements):
        if next_shard_xml is None or (not single_shard and random.randint(1, 3) == 1):
            next_shard_xml = ET.SubElement(next_cluster, "shard")
            # Add internal replication
            if random.randint(1, 100) <= 40:
                internal_replication_xml = ET.SubElement(
                    next_shard_xml, "internal_replication"
                )
                internal_replication_xml.text = (
                    "false" if random.randint(1, 4) <= 3 else "true"
                )
        next_replica_xml = ET.SubElement(next_shard_xml, "replica")
        next_host_xml = ET.SubElement(next_replica_xml, "host")
        next_host_xml.text = input_nodes[j]
        next_port_xml = ET.SubElement(next_replica_xml, "port")
        if random.randint(1, 100) <= 25:
            secure_xml = ET.SubElement(next_replica_xml, "secure")
            secure_xml.text = "1"
            next_port_xml.text = "9440"
        else:
            next_port_xml.text = "9000"


def add_single_disk(
    i: int,
    args,
    cluster: ClickHouseCluster,
    next_disk: ET.Element,
    backups_element: ET.Element,
    disk_type: str,
    created_disks_types: list[tuple[int, str]],
    is_private_binary: bool,
) -> tuple[int, str]:
    prev_disk = 0
    if disk_type in ("cache", "encrypted"):
        iter_prev_disk = prev_disk = random.choice(range(0, i))

        # Cannot create encrypted disk on web storage
        # Cannot create cached disk on local storage or encrypted disk
        while created_disks_types[iter_prev_disk][1] in ("cache", "encrypted"):
            if (
                created_disks_types[iter_prev_disk][1] == "encrypted"
                and disk_type == "cache"
            ):
                disk_type = "object_storage"
                break
            iter_prev_disk = created_disks_types[iter_prev_disk][0]
        if created_disks_types[iter_prev_disk][1] == "web" and disk_type == "encrypted":
            disk_type = "cache"
        elif created_disks_types[iter_prev_disk][1] == "local" and disk_type == "cache":
            disk_type = "object_storage"

    # Add a single disk
    disk_type_xml = ET.SubElement(next_disk, "type")
    disk_type_xml.text = disk_type

    allowed_disk_xml = ET.SubElement(backups_element, "allowed_disk")
    allowed_disk_xml.text = f"disk{i}"
    final_type = disk_type

    if disk_type == "object_storage":
        object_storages = ["local"]
        if args.with_minio:
            object_storages.append("s3")
            if is_private_binary:
                # Increased probability
                object_storages.extend(
                    ["s3_with_keeper", "s3_with_keeper", "s3_with_keeper"]
                )
        if args.with_azurite:
            object_storages.append("azure")
        if args.with_nginx:
            object_storages.append("web")
        final_type = object_storage_type = random.choice(object_storages)
        object_storage_type_xml = ET.SubElement(next_disk, "object_storage_type")
        object_storage_type_xml.text = object_storage_type

        # Set disk metadata type
        metadata_type = "keeper" if object_storage_type == "s3_with_keeper" else "local"
        if random.randint(1, 100) <= 70:
            possible_metadata_types = (
                ["local", "plain", "web"]
                if object_storage_type == "web"
                else ["local", "plain", "plain_rewritable"]
            )
            if is_private_binary and object_storage_type != "web":
                # Increased probability
                possible_metadata_types.extend(["keeper", "keeper", "keeper"])
            metadata_type = random.choice(possible_metadata_types)
        metadata_xml = ET.SubElement(next_disk, "metadata_type")
        metadata_xml.text = metadata_type

        # Add endpoint info
        if object_storage_type in ("s3", "s3_with_keeper"):
            endpoint_xml = ET.SubElement(next_disk, "endpoint")
            endpoint_xml.text = f"http://minio1:9001/root/data{i}"
            access_key_id_xml = ET.SubElement(next_disk, "access_key_id")
            access_key_id_xml.text = "minio"
            secret_access_key_xml = ET.SubElement(next_disk, "secret_access_key")
            secret_access_key_xml.text = "ClickHouse_Minio_P@ssw0rd"
        elif object_storage_type == "azure":
            endpoint_xml = ET.SubElement(next_disk, "endpoint")
            endpoint_xml.text = (
                f"http://azurite1:{cluster.azurite_port}/devstoreaccount1/data{i}"
            )
            account_name_xml = ET.SubElement(next_disk, "account_name")
            account_name_xml.text = "devstoreaccount1"
            account_key_xml = ET.SubElement(next_disk, "account_key")
            account_key_xml.text = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
        elif object_storage_type == "web":
            endpoint_xml = ET.SubElement(next_disk, "endpoint")
            endpoint_xml.text = f"http://nginx:80/data{i}/"
        elif object_storage_type == "local":
            path_xml = ET.SubElement(next_disk, "path")
            path_xml.text = f"/var/lib/clickhouse/disk{i}/"
            allowed_path_xml = ET.SubElement(backups_element, "allowed_path")
            allowed_path_xml.text = f"/var/lib/clickhouse/disk{i}/"

        # Add a endpoint_subpath
        if metadata_type == "plain_rewritable" and random.randint(1, 100) <= 70:
            endpoint_subpath_xml = ET.SubElement(next_disk, "endpoint_subpath")
            endpoint_subpath_xml.text = (
                f"node{random.choice(range(0, len(args.replica_values)))}"
            )

        # Add storage settings
        dict_entry = (
            "s3" if object_storage_type == "s3_with_keeper" else object_storage_type
        )
        if object_storages_properties[dict_entry] and random.randint(1, 100) <= 70:
            add_settings_from_dict(object_storages_properties[dict_entry], next_disk)
    elif disk_type in ("cache", "encrypted"):
        disk_xml = ET.SubElement(next_disk, "disk")
        disk_xml.text = f"disk{prev_disk}"
        if disk_type == "cache" or random.randint(1, 2) == 1:
            path_xml = ET.SubElement(next_disk, "path")
            path_xml.text = f"/var/lib/clickhouse/disk{i}/"
            allowed_path_xml = ET.SubElement(backups_element, "allowed_path")
            allowed_path_xml.text = f"/var/lib/clickhouse/disk{i}/"

        if disk_type == "cache":
            max_size_xml = ET.SubElement(next_disk, "max_size")
            max_size_xml.text = file_size_value(100)()

            # Add random settings
            if random.randint(1, 100) <= 70:
                add_settings_from_dict(cache_storage_properties, next_disk)
        else:
            enc_algorithm = random.choice(["aes_128_ctr", "aes_192_ctr", "aes_256_ctr"])
            algorithm_xml = ET.SubElement(next_disk, "algorithm")
            algorithm_xml.text = enc_algorithm

            if enc_algorithm == "aes_128_ctr":
                key_xml = ET.SubElement(next_disk, "key")
                key_xml.text = "".join([random.choice("0123456789") for _ in range(16)])
            else:
                key_hex_xml = ET.SubElement(next_disk, "key_hex")
                key_hex_xml.text = "".join(
                    [
                        random.choice("abcdef0123456789")
                        for _ in range(48 if enc_algorithm == "aes_192_ctr" else 64)
                    ]
                )

    if random.randint(1, 100) <= 50:
        add_settings_from_dict(all_disks_properties, next_disk)
    return (prev_disk, final_type)


def add_single_cache(i: int, next_cache: ET.Element):
    max_size_xml = ET.SubElement(next_cache, "max_size")
    max_size_xml.text = file_size_value(10)()
    path_xml = ET.SubElement(next_cache, "path")
    path_xml.text = f"/var/lib/clickhouse/fcache{i}/"

    # Add random settings
    if random.randint(1, 100) <= 70:
        add_settings_from_dict(cache_storage_properties, next_cache)


def add_ssl_settings(next_ssl: ET.Element):
    certificate_xml = ET.SubElement(next_ssl, "certificateFile")
    private_key_xml = ET.SubElement(next_ssl, "privateKeyFile")
    if random.randint(1, 2) == 1:
        certificate_xml.text = "/etc/clickhouse-server/config.d/server.crt"
        private_key_xml.text = "/etc/clickhouse-server/config.d/server.key"
    else:
        certificate_xml.text = "/etc/clickhouse-server/config.d/server-cert.pem"
        private_key_xml.text = "/etc/clickhouse-server/config.d/server-key.pem"
        ca_config_xml = ET.SubElement(next_ssl, "caConfig")
        ca_config_xml.text = "/etc/clickhouse-server/config.d/ca-cert.pem"
    if random.randint(1, 2) == 1:
        dh_params_xml = ET.SubElement(next_ssl, "dhParamsFile")
        dh_params_xml.text = "/etc/clickhouse-server/config.d/dhparam.pem"

    if random.randint(1, 2) == 1:
        verification_xml = ET.SubElement(next_ssl, "verificationMode")
        verification_xml.text = random.choice(["none", "relaxed", "strict"])
    if random.randint(1, 2) == 1:
        load_ca_xml = ET.SubElement(next_ssl, "loadDefaultCAFile")
        load_ca_xml.text = random.choice(["true", "false"])
    if random.randint(1, 2) == 1:
        cache_sessions_xml = ET.SubElement(next_ssl, "cacheSessions")
        cache_sessions_xml.text = random.choice(["true", "false"])
    if random.randint(1, 2) == 1:
        prefer_server_ciphers_xml = ET.SubElement(next_ssl, "preferServerCiphers")
        prefer_server_ciphers_xml.text = random.choice(["true", "false"])
    if random.randint(1, 2) == 1:
        SSL_PROTOCOLS = ["sslv2", "sslv3", "tlsv1", "tlsv1_1", "tlsv1_2"]
        disabled_list_str = ""
        shuffled_protocols = list(SSL_PROTOCOLS)  # Do copy
        random.shuffle(shuffled_protocols)
        for i in range(0, random.randint(1, len(shuffled_protocols))):
            disabled_list_str += "" if i == 0 else ","
            disabled_list_str += shuffled_protocols[i]
        disabled_protocols_xml = ET.SubElement(next_ssl, "disableProtocols")
        disabled_protocols_xml.text = disabled_list_str


def modify_server_settings(
    args,
    cluster: ClickHouseCluster,
    number_replicas: int,
    is_private_binary: bool,
    input_config_path: str,
) -> tuple[bool, str, int]:
    modified = False
    number_clusters = 0
    removed_default_cluster = False

    # Parse the existing XML file
    tree = ET.parse(input_config_path)
    root = tree.getroot()
    if root.tag != "clickhouse":
        raise Exception("<clickhouse> element not found")

    if root.find("tcp_port_secure") is None:
        modified = True
        secure_port_xml = ET.SubElement(root, "tcp_port_secure")
        secure_port_xml.text = "9440"
    if root.find("https_port") is None:
        modified = True
        secure_port_xml = ET.SubElement(root, "https_port")
        secure_port_xml.text = "8443"
    if root.find("openSSL") is None:
        modified = True
        openssl_xml = ET.SubElement(root, "openSSL")
        server_xml = ET.SubElement(openssl_xml, "server")
        add_ssl_settings(server_xml)

        client_xml = ET.SubElement(openssl_xml, "client")
        add_ssl_settings(client_xml)
        if random.randint(1, 2) == 1:
            invalid_handler_xml = ET.SubElement(client_xml, "invalidCertificateHandler")
            name_xml = ET.SubElement(invalid_handler_xml, "name")
            name_xml.text = random.choice(
                ["AcceptCertificateHandler", "RejectCertificateHandler"]
            )

    # Add remote server configurations
    if (
        root.find("remote_servers") is None
        and random.randint(1, 100) <= args.add_remote_server_settings_prob
    ):
        modified = True

        existing_nodes = [f"node{i}" for i in range(0, number_replicas)]
        remote_server_config = ET.SubElement(root, "remote_servers")

        # Remove default cluster
        if random.randint(1, 2) == 1:
            removed_default_cluster = True
            default_cluster = ET.SubElement(
                remote_server_config, "default", attrib={"remove": "remove"}
            )
            default_cluster.text = ""

        lower_bound, upper_bound = args.number_servers
        number_clusters = random.randint(lower_bound, upper_bound)
        for i in range(0, number_clusters):
            add_single_cluster(
                existing_nodes,
                ET.SubElement(remote_server_config, f"cluster{i}"),
            )

    # Add disk configurations
    if (
        root.find("storage_configuration") is None
        and random.randint(1, 100) <= args.add_disk_settings_prob
    ):
        modified = True

        storage_config = ET.SubElement(root, "storage_configuration")
        disk_element = ET.SubElement(storage_config, "disks")
        backups_element = ET.SubElement(root, "backups")
        lower_bound, upper_bound = args.number_disks
        number_disks = random.randint(lower_bound, upper_bound)
        number_policies = 0

        allowed_disk_xml = ET.SubElement(backups_element, "allowed_disk")
        allowed_disk_xml.text = "default"
        created_disks_types = []
        created_cache_disks = []

        for i in range(0, number_disks):
            possible_types = (
                ["object_storage"]
                if i == 0
                else ["object_storage", "object_storage", "cache", "encrypted"]
            )
            next_created_disk_pair = add_single_disk(
                i,
                args,
                cluster,
                ET.SubElement(disk_element, f"disk{i}"),
                backups_element,
                random.choice(possible_types),
                created_disks_types,
                is_private_binary,
            )
            created_disks_types.append(next_created_disk_pair)
            if next_created_disk_pair[1] == "cache":
                created_cache_disks.append(i)
        # Add policies sometimes
        if random.randint(1, 100) <= args.add_policy_settings_prob:
            j = 0
            bottom_disks = []
            for val in created_disks_types:
                if val[1] not in ("cache", "encrypted"):
                    bottom_disks.append(j)
                j += 1
            number_bottom_disks = len(bottom_disks)
            policies_element = ET.SubElement(storage_config, "policies")
            lower_bound, upper_bound = args.number_disks
            number_policies = random.randint(lower_bound, upper_bound)

            for i in range(0, number_policies):
                next_policy_xml = ET.SubElement(policies_element, f"policy{i}")
                volumes_xml = ET.SubElement(next_policy_xml, "volumes")
                main_xml = None
                volume_counter = 0

                number_elements = (
                    1
                    if random.randint(1, 2) == 1
                    else random.randint(1, number_bottom_disks)
                )
                input_disks = list(bottom_disks)  # Do copy
                random.shuffle(input_disks)
                for i in range(0, number_elements):
                    if main_xml is None or random.randint(1, 3) == 1:
                        if main_xml is not None and random.randint(1, 100) <= 70:
                            add_settings_from_dict(policy_properties, main_xml)
                        main_xml = ET.SubElement(volumes_xml, f"volume{volume_counter}")
                        volume_counter += 1
                    disk_xml = ET.SubElement(main_xml, "disk")
                    disk_xml.text = f"disk{input_disks[i]}"
                if main_xml is not None and random.randint(1, 100) <= 70:
                    add_settings_from_dict(policy_properties, main_xml)
                if random.randint(1, 100) <= 70:
                    add_settings_from_dict(policy_properties, next_policy_xml)

        allowed_path_xml1 = ET.SubElement(backups_element, "allowed_path")
        allowed_path_xml1.text = "/var/lib/clickhouse/"
        allowed_path_xml2 = ET.SubElement(backups_element, "allowed_path")
        allowed_path_xml2.text = "/var/lib/clickhouse/user_files/"

        if (
            root.find("temporary_data_in_cache") is None
            and root.find("tmp_policy") is None
            and root.find("tmp_path") is None
        ):
            next_opt = random.randint(1, 100)

            if len(created_cache_disks) > 0 and next_opt <= 40:
                temporary_cache_xml = ET.SubElement(root, "temporary_data_in_cache")
                temporary_cache_xml.text = f"disk{random.choice(created_cache_disks)}"
            # elif number_policies > 0 and next_opt <= 70: the disks must be local
            #    tmp_policy_xml = ET.SubElement(root, "tmp_policy")
            #    tmp_policy_xml.text = (
            #        f"policy{random.choice(range(0, number_policies))}"
            #    )
            else:
                tmp_path_xml = ET.SubElement(root, "tmp_path")
                tmp_path_xml.text = "/var/lib/clickhouse/tmp/"

    # Add filesystem caches
    if (
        root.find("filesystem_caches") is None
        and random.randint(1, 100) <= args.add_filesystem_caches_prob
    ):
        modified = True
        filesystem_caches_config = ET.SubElement(root, "filesystem_caches")

        lower_bound, upper_bound = args.number_caches
        number_caches = random.randint(lower_bound, upper_bound)
        for i in range(0, number_caches):
            add_single_cache(i, ET.SubElement(filesystem_caches_config, f"fcache{i}"))

    # Add keeper_map_path_prefix
    if args.add_keeper_map_prefix and root.find("keeper_map_path_prefix") is None:
        modified = True
        new_element = ET.SubElement(root, "keeper_map_path_prefix")
        new_element.text = "/keeper_map_tables"
    # Add experimental transactions
    if args.add_transactions and root.find("allow_experimental_transactions") is None:
        modified = True
        new_element = ET.SubElement(root, "allow_experimental_transactions")
        new_element.text = "1"

    # Add distributed_ddl
    if args.add_distributed_ddl and root.find("distributed_ddl") is None:
        modified = True
        distributed_ddl_xml = ET.SubElement(root, "distributed_ddl")
        path_xml = ET.SubElement(distributed_ddl_xml, "path")
        path_xml.text = "/clickhouse/task_queue/ddl"
        replicas_path_xml = ET.SubElement(distributed_ddl_xml, "replicas_path")
        replicas_path_xml.text = "/clickhouse/task_queue/replicas"
        if random.randint(1, 100) <= 70:
            modified = (
                apply_properties_recursively(root, distributed_ddl_properties)
                or modified
            )

    # Select random properties to the XML
    if random.randint(1, 100) <= args.server_settings_prob:
        if is_private_binary and "shared_database_catalog" not in possible_properties:
            # Add shared_database_catalog settings
            shared_settings = {
                "delay_before_drop_intention_seconds": threshold_generator(
                    0.2, 0.2, 0, 60
                ),
                "delay_before_drop_table_seconds": threshold_generator(0.2, 0.2, 0, 60),
                "drop_local_thread_pool_size": threads_lambda,
                "drop_lock_duration_seconds": threshold_generator(0.2, 0.2, 0, 60),
                "drop_zookeeper_thread_pool_size": threads_lambda,
                "migration_from_database_replicated": true_false_lambda,
                "state_application_thread_pool_size": threads_lambda,
            }
            if number_clusters > 0 and random.randint(1, 100) <= 75:
                cluster_name_choices = [
                    f"cluster{i}" for i in range(0, number_clusters)
                ]
                if not removed_default_cluster:
                    cluster_name_choices.append("default")
                shared_settings["cluster_name"] = lambda: random.choice(
                    cluster_name_choices
                )
            possible_properties["shared_database_catalog"] = shared_settings
        modified = apply_properties_recursively(root, possible_properties) or modified
        if modified:
            # Make sure `path` in distributed_ddl is set
            distributed_ddl_xml = root.find("distributed_ddl")
            if (
                distributed_ddl_xml is not None
                and distributed_ddl_xml.find("path") is None
            ):
                path_xml = ET.SubElement(distributed_ddl_xml, "path")
                path_xml.text = "/var/lib/clickhouse/task_queue/ddl"
            # Make sure `zookeeper_path` in transaction_log is set
            transaction_log_xml = root.find("transaction_log")
            if (
                transaction_log_xml is not None
                and transaction_log_xml.find("zookeeper_path") is None
            ):
                zookeeper_path_xml = ET.SubElement(
                    transaction_log_xml, "zookeeper_path"
                )
                zookeeper_path_xml.text = "/var/lib/clickhouse/txn"

    if modified:
        ET.indent(tree, space="    ", level=0)  # indent tree
        temp_path = None
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as temp_file:
            temp_path = temp_file.name
            # Write the modified XML to the temporary file
            tree.write(temp_path, encoding="utf-8", xml_declaration=True)
        return True, temp_path, number_clusters
    return False, input_config_path, number_clusters


def modify_user_settings(
    input_config_path: str, number_clusters: int
) -> tuple[bool, str]:
    modified = False

    # Parse the existing XML file
    tree = ET.parse(input_config_path)
    root = tree.getroot()
    if root.tag != "clickhouse":
        raise Exception("<clickhouse> element not found")

    if number_clusters > 0:
        modified = True
        profiles_xml = root.find("profiles")
        if profiles_xml is None:
            profiles_xml = ET.SubElement(root, "profiles")
        default_xml = profiles_xml.find("default")
        if default_xml is None:
            default_xml = ET.SubElement(profiles_xml, "default")
        cluster_for_parallel_replicas_xml = default_xml.find(
            "cluster_for_parallel_replicas"
        )
        if cluster_for_parallel_replicas_xml is None:
            cluster_for_parallel_replicas_xml = ET.SubElement(
                default_xml, "cluster_for_parallel_replicas"
            )
        cluster_for_parallel_replicas_xml.text = (
            f"cluster{random.choice(range(0, number_clusters))}"
        )

    if modified:
        ET.indent(tree, space="    ", level=0)  # indent tree
        temp_path = None
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as temp_file:
            temp_path = temp_file.name
            # Write the modified XML to the temporary file
            tree.write(temp_path, encoding="utf-8", xml_declaration=True)
        return True, temp_path
    return False, input_config_path
