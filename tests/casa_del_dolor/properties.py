from abc import abstractmethod
import xml.etree.ElementTree as ET
import tempfile
import multiprocessing
import random
import string
import typing

from environment import get_system_timezones
from integration.helpers.cluster import ClickHouseCluster


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


def threshold_generator(
    always_on_prob, always_off_prob, min_val, max_val, bits: int = 64
):
    def gen():
        tmp = random.random()
        if tmp <= always_on_prob:
            return min_val
        if tmp <= always_on_prob + always_off_prob:
            return max_val
        if (
            tmp <= always_on_prob + always_off_prob + 0.01
            and isinstance(min_val, int)
            and isinstance(max_val, int)
        ):
            return 2**bits - 1

        if isinstance(min_val, int) and isinstance(max_val, int):
            return random.randint(min_val, max_val)
        return random.uniform(min_val, max_val)

    return gen


def file_size_value(max_val: int, bits: int = 64):
    def gen():
        return str(threshold_generator(0.2, 0.2, 1, max_val, bits)()) + random.choice(
            ["Ki", "Ki", "Mi", "Gi"]  # Increased probability
        )

    return gen


true_false_lambda = lambda: random.randint(0, 1)
threads_lambda = lambda: random.randint(0, multiprocessing.cpu_count())
no_zero_threads_lambda = lambda: random.randint(1, multiprocessing.cpu_count())


rocksdb_properties = {
    "rocksdb": {
        "options": {
            "max_background_jobs": threshold_generator(0.2, 0.2, 0, 100),
        }
    }
}


possible_properties = {
    "access_control_improvements": {
        "on_cluster_queries_require_cluster_grant": true_false_lambda,
        "role_cache_expiration_time_seconds": threshold_generator(0.2, 0.2, 1, 60, 31),
        "select_from_information_schema_requires_grant": true_false_lambda,
        "select_from_system_db_requires_grant": true_false_lambda,
        "settings_constraints_replace_previous": true_false_lambda,
        "table_engines_require_grant": true_false_lambda,
        "users_without_row_policies_can_read_rows": true_false_lambda,
    },
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
    "asynchronous_heavy_metrics_update_period_s": threshold_generator(0.2, 0.2, 1, 60),
    "asynchronous_metrics_enable_heavy_metrics": true_false_lambda,
    "asynchronous_metrics_keeper_metrics_only": true_false_lambda,
    "asynchronous_metrics_update_period_s": threshold_generator(0.2, 0.2, 1, 30),
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
    "bcrypt_workfactor": threshold_generator(0.2, 0.2, 0, 20, 31),
    "cache_size_to_ram_max_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    # "cannot_allocate_thread_fault_injection_probability": threshold_generator(0.2, 0.2, 0.0, 1.0), the server may not start
    "compiled_expression_cache_elements_size": threshold_generator(0.2, 0.2, 0, 10000),
    "compiled_expression_cache_size": threshold_generator(0.2, 0.2, 0, 10000),
    "concurrent_threads_scheduler": lambda: random.choice(
        ["round_robin", "fair_round_robin"]
    ),
    "concurrent_threads_soft_limit_num": threads_lambda,
    "concurrent_threads_soft_limit_ratio_to_cores": threads_lambda,
    "database_catalog_drop_table_concurrency": threads_lambda,
    "database_replicated_allow_detach_permanently": true_false_lambda,
    "database_replicated_drop_broken_tables": true_false_lambda,
    "dictionaries_lazy_load": true_false_lambda,
    "disable_insertion_and_mutation": true_false_lambda,
    "disable_internal_dns_cache": true_false_lambda,
    "display_secrets_in_show_and_select": true_false_lambda,
    "distributed_cache_keep_up_free_connections_ratio": threshold_generator(
        0.2, 0.2, 0.0, 1.0
    ),
    "enable_azure_sdk_logging": true_false_lambda,
    "format_alter_operations_with_parentheses": true_false_lambda,
    "iceberg_catalog_threadpool_pool_size": threads_lambda,
    "iceberg_catalog_threadpool_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "iceberg_metadata_files_cache_max_entries": threshold_generator(0.2, 0.2, 0, 10000),
    "iceberg_metadata_files_cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "iceberg_metadata_files_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "iceberg_metadata_files_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
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
    "max_named_collection_num_to_throw": threshold_generator(0.2, 0.2, 0, 1000),
    # "max_temporary_data_on_disk_size": threshold_generator(0.2, 0.2, 0, 1000), not worth to mess around
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
    "os_threads_nice_value_distributed_cache_tcp_handler": threshold_generator(
        0.2, 0.2, -20, 19
    ),
    "os_threads_nice_value_merge_mutate": threshold_generator(0.2, 0.2, -20, 19),
    "os_threads_nice_value_zookeeper_client_send_receive": threshold_generator(
        0.2, 0.2, -20, 19
    ),
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
    "prefetch_threadpool_pool_size": threshold_generator(0.2, 0.2, 1, 1000),
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
    "s3queue_disable_streaming": true_false_lambda,
    "shutdown_wait_backups_and_restores": true_false_lambda,
    "shutdown_wait_unfinished_queries": true_false_lambda,
    "startup_mv_delay_ms": threshold_generator(0.2, 0.2, 0, 1000),
    "storage_shared_set_join_use_inner_uuid": true_false_lambda,
    "tables_loader_background_pool_size": threads_lambda,
    "tables_loader_foreground_pool_size": threads_lambda,
    "temporary_data_in_distributed_cache": true_false_lambda,
    "thread_pool_queue_size": threshold_generator(0.2, 0.2, 0, 1000),
    "threadpool_writer_pool_size": threshold_generator(0.2, 0.2, 1, 200),
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
    **rocksdb_properties,
}

distributed_properties = {
    "cleanup_delay_period": threshold_generator(0.2, 0.2, 0, 60),
    "max_tasks_in_queue": threshold_generator(0.2, 0.2, 0, 1000),
    "pool_size": no_zero_threads_lambda,
    "task_max_lifetime": threshold_generator(0.2, 0.2, 0, 60),
}

object_storages_properties = {
    "local": {},
    "s3": {
        "list_object_keys_size": threshold_generator(0.2, 0.2, 1, 10 * 1024 * 1024, 31),
        "metadata_keep_free_space_bytes": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "objects_chunk_size_to_delete": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024, 32
        ),
        "object_metadata_cache_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "remove_shared_recursive_file_limit": threshold_generator(0.2, 0.2, 0, 31),
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
        "list_object_keys_size": threshold_generator(0.2, 0.2, 1, 10 * 1024 * 1024),
        "max_single_download_retries": threshold_generator(0.2, 0.2, 0, 16),
        "max_single_part_upload_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "max_single_read_retries": threshold_generator(0.2, 0.2, 0, 16),
        "metadata_keep_free_space_bytes": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "min_upload_part_size": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
        "objects_chunk_size_to_delete": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "remove_shared_recursive_file_limit": threshold_generator(0.2, 0.2, 0, 31),
        "skip_access_check": true_false_lambda,
        "thread_pool_size": threads_lambda,
        "use_native_copy": true_false_lambda,
    },
    "web": {},
}

s3_with_keeper_properties = {
    "metadata_cache_cleanup_interval": threshold_generator(0.2, 0.2, 1, 60),
    "metadata_cache_enabled": true_false_lambda,
    "metadata_cache_full_directory_lists": true_false_lambda,
}

metadata_cleanup_properties = {
    "enabled": lambda: 1 if random.randint(0, 9) < 9 else 0,
    "deleted_objects_delay_sec": threshold_generator(0.2, 0.2, 0, 60),
    "old_transactions_delay_sec": threshold_generator(0.2, 0.2, 0, 60),
    "interval_sec": threshold_generator(0.2, 0.2, 1, 60),
}


cache_storage_properties = {
    "allow_dynamic_cache_resize": true_false_lambda,
    "background_download_max_file_segment_size": threshold_generator(
        0.2, 0.2, 0, 10 * 1024 * 1024
    ),
    "background_download_queue_size_limit": threshold_generator(0.2, 0.2, 0, 128),
    "background_download_threads": threads_lambda,
    "boundary_alignment": threshold_generator(0.2, 0.2, 0, 128),
    "cache_on_write_operations": true_false_lambda,
    "enable_bypass_cache_with_threshold": true_false_lambda,
    "enable_filesystem_query_cache_limit": true_false_lambda,
    "keep_free_space_elements_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "keep_free_space_remove_batch": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    "keep_free_space_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "load_metadata_asynchronously": true_false_lambda,
    "load_metadata_threads": threads_lambda,
    "max_elements": threshold_generator(0.2, 0.2, 3, 10000000),
    "max_file_segment_size": file_size_value(100),
    # "max_size_ratio_to_total_space": threshold_generator(0.2, 0.2, 0.0, 1.0), cannot be specified with `max_size` at the same time
    "slru_size_ratio": threshold_generator(0.2, 0.2, 0.01, 0.99),
    "write_cache_per_user_id_directory": true_false_lambda,
}


policy_properties = {
    "description": lambda: generate_xml_safe_string(random.randint(1, 1024)),
    "load_balancing": lambda: random.choice(["round_robin", "least_used"]),
    "max_data_part_size_bytes": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    "move_factor": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "perform_ttl_move_on_insert": true_false_lambda,
    "prefer_not_to_merge": true_false_lambda,
}


all_disks_properties = {
    "description": lambda: generate_xml_safe_string(random.randint(1, 1024)),
    "keep_free_space_bytes": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    "min_bytes_for_seek": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024),
    "perform_ttl_move_on_insert": true_false_lambda,
    "readonly": lambda: 1 if random.randint(0, 9) < 2 else 0,
    "skip_access_check": true_false_lambda,
}


backup_properties = {
    "allow_concurrent_backups": true_false_lambda,
    "allow_concurrent_restores": true_false_lambda,
    "remove_backup_files_after_failure": true_false_lambda,
    "test_randomize_order": true_false_lambda,
    "test_inject_sleep": true_false_lambda,
}


class PropertiesGroup:
    def __init__(self):
        pass

    @abstractmethod
    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        pass


Parameter = typing.Callable[[], int | float] | PropertiesGroup


def sample_from_dict(d: dict[str, Parameter], sample: int) -> dict[str, Parameter]:
    items = random.sample(list(d.items()), sample)
    return dict(items)


def apply_properties_recursively(
    next_root: ET.Element, next_properties: dict[str, Parameter], min_values: int = 1
):
    is_modified = False
    selected_props = sample_from_dict(
        next_properties, random.randint(min_values, len(next_properties))
    )
    for setting, next_child in selected_props.items():
        if next_root.find(setting) is None:
            is_modified = True
            new_element = ET.SubElement(next_root, setting)
            if isinstance(next_child, dict):
                apply_properties_recursively(new_element, next_child, min_values)
            elif isinstance(next_child, PropertiesGroup):
                raise Exception("Can't use Properties Group here")
            else:
                new_element.text = str(next_child())
    return is_modified


def remove_element(property_element: ET.Element, elem: str):
    remove_xml = ET.SubElement(property_element, elem, attrib={"remove": "remove"})
    remove_xml.text = ""


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


class ClusterPropertiesGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        # remote_server_config = ET.SubElement(root, "remote_servers")
        existing_nodes = [f"node{i}" for i in range(0, len(args.replica_values))]

        # Remove default clusters
        if random.randint(1, 2) == 1:
            remove_element(property_element, "default")
        if random.randint(1, 2) == 1:
            remove_element(property_element, "all_groups.default")

        lower_bound, upper_bound = args.number_servers
        number_clusters = random.randint(lower_bound, upper_bound)
        for i in range(0, number_clusters):
            add_single_cluster(
                existing_nodes,
                ET.SubElement(property_element, f"cluster{i}"),
            )


def add_single_disk(
    i: int,
    args,
    cluster: ClickHouseCluster,
    next_disk: ET.Element,
    backups_element: ET.Element,
    disk_type: str,
    created_disks_types: list[tuple[int, str]],
    is_private_binary: bool,
) -> tuple[int, str, str]:
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
    final_super_type = disk_type

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
            endpoint_xml.text = f"http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/data{i}"
            access_key_id_xml = ET.SubElement(next_disk, "access_key_id")
            access_key_id_xml.text = "minio"
            secret_access_key_xml = ET.SubElement(next_disk, "secret_access_key")
            secret_access_key_xml.text = cluster.minio_secret_key
        elif object_storage_type == "azure":
            endpoint_xml = ET.SubElement(next_disk, "endpoint")
            endpoint_xml.text = f"http://{cluster.azurite_host}:{cluster.azurite_port}/{cluster.azurite_account}/data{i}"
            account_name_xml = ET.SubElement(next_disk, "account_name")
            account_name_xml.text = cluster.azurite_account
            account_key_xml = ET.SubElement(next_disk, "account_key")
            account_key_xml.text = cluster.azurite_key
        elif object_storage_type == "web":
            endpoint_xml = ET.SubElement(next_disk, "endpoint")
            endpoint_xml.text = (
                f"http://{cluster.nginx_host}:{cluster.nginx_port}/data{i}/"
            )
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
            apply_properties_recursively(
                next_disk, object_storages_properties[dict_entry]
            )
        if (
            is_private_binary
            and (object_storage_type == "s3_with_keeper" or metadata_type == "keeper")
            and random.randint(1, 100) <= 70
        ):
            metadata_xml = ET.SubElement(next_disk, "metadata_background_cleanup")
            apply_properties_recursively(metadata_xml, metadata_cleanup_properties)
    elif disk_type == "s3_with_keeper":
        endpoint_xml = ET.SubElement(next_disk, "endpoint")
        endpoint_xml.text = f"http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/data{i}"
        access_key_id_xml = ET.SubElement(next_disk, "access_key_id")
        access_key_id_xml.text = "minio"
        secret_access_key_xml = ET.SubElement(next_disk, "secret_access_key")
        secret_access_key_xml.text = cluster.minio_secret_key

        # Add storage settings
        if random.randint(1, 100) <= 70:
            apply_properties_recursively(next_disk, s3_with_keeper_properties)
        if random.randint(1, 100) <= 70:
            metadata_xml = ET.SubElement(next_disk, "metadata_background_cleanup")
            apply_properties_recursively(metadata_xml, metadata_cleanup_properties)
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
            max_size_xml.text = file_size_value(100, 4)()

            # Add random settings
            if random.randint(1, 100) <= 70:
                apply_properties_recursively(next_disk, cache_storage_properties)
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

    if disk_type != "cache" and random.randint(1, 100) <= 50:
        apply_properties_recursively(next_disk, all_disks_properties)
    return (prev_disk, final_type, final_super_type)


class DiskPropertiesGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        disk_element = ET.SubElement(property_element, "disks")
        backups_element = ET.SubElement(top_root, "backups")
        disks_table_engines = ET.SubElement(top_root, "allowed_disks_for_table_engines")
        lower_bound, upper_bound = args.number_disks
        number_disks = random.randint(lower_bound, upper_bound)
        number_policies = 0

        allowed_disk_xml = ET.SubElement(backups_element, "allowed_disk")
        allowed_disk_xml.text = "default"
        created_disks_types = []
        created_cache_disks = []
        created_keeper_disks = []

        for i in range(0, number_disks):
            possible_types = (
                ["object_storage"]
                if i == 0
                else ["object_storage", "object_storage", "cache", "encrypted"]
            )
            if args.with_minio and is_private_binary:
                # Increase probability
                possible_types.extend(
                    ["s3_with_keeper", "s3_with_keeper", "s3_with_keeper"]
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
            elif (
                is_private_binary
                and args.set_shared_mergetree_disk
                and next_created_disk_pair[2] == "s3_with_keeper"
            ):
                created_keeper_disks.append(i)

        # Allow any disk in any table engine
        disks_table_engines.text = ",".join(
            ["default"] + [f"disk{i}" for i in range(0, number_disks)]
        )
        # Add policies sometimes
        if random.randint(1, 100) <= args.add_policy_settings_prob:
            j = 0
            bottom_disks = []
            for val in created_disks_types:
                if val[1] not in ("cache", "encrypted"):
                    bottom_disks.append(j)
                j += 1
            number_bottom_disks = len(bottom_disks)
            policies_element = ET.SubElement(property_element, "policies")
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
                            apply_properties_recursively(main_xml, policy_properties)
                        main_xml = ET.SubElement(volumes_xml, f"volume{volume_counter}")
                        volume_counter += 1
                    disk_xml = ET.SubElement(main_xml, "disk")
                    disk_xml.text = f"disk{input_disks[i]}"
                if main_xml is not None and random.randint(1, 100) <= 70:
                    apply_properties_recursively(main_xml, policy_properties)
                if random.randint(1, 100) <= 70:
                    apply_properties_recursively(next_policy_xml, policy_properties)

        allowed_path_xml1 = ET.SubElement(backups_element, "allowed_path")
        allowed_path_xml1.text = "/var/lib/clickhouse/"
        allowed_path_xml2 = ET.SubElement(backups_element, "allowed_path")
        allowed_path_xml2.text = "/var/lib/clickhouse/user_files/"
        if random.randint(1, 100) <= 70:
            apply_properties_recursively(backups_element, backup_properties)

        if (
            top_root.find("temporary_data_in_cache") is None
            and top_root.find("tmp_policy") is None
            and top_root.find("tmp_path") is None
        ):
            next_opt = random.randint(1, 100)

            if len(created_cache_disks) > 0 and next_opt <= 40:
                temporary_cache_xml = ET.SubElement(top_root, "temporary_data_in_cache")
                temporary_cache_xml.text = f"disk{random.choice(created_cache_disks)}"
            # elif number_policies > 0 and next_opt <= 70: the disks must be local
            #    tmp_policy_xml = ET.SubElement(root, "tmp_policy")
            #    tmp_policy_xml.text = (
            #        f"policy{random.choice(range(0, number_policies))}"
            #    )
            else:
                tmp_path_xml = ET.SubElement(top_root, "tmp_path")
                tmp_path_xml.text = "/var/lib/clickhouse/tmp/"
        # Set disk for SMTs
        if len(created_keeper_disks) > 0:
            smt_element = ET.SubElement(top_root, "shared_merge_tree")
            disk_element = ET.SubElement(smt_element, "disk")
            disk_element.text = f"disk{random.choice(created_keeper_disks)}"
        # Optionally set database disk
        if number_disks > 0 and random.randint(1, 100) <= 30:
            dbd_element = ET.SubElement(top_root, "database_disk")
            disk_element = ET.SubElement(dbd_element, "disk")
            disk_element.text = f"disk{random.randint(0, number_disks - 1)}"


def add_single_cache(i: int, next_cache: ET.Element):
    max_size_xml = ET.SubElement(next_cache, "max_size")
    max_size_xml.text = file_size_value(10, 4)()
    path_xml = ET.SubElement(next_cache, "path")
    path_xml.text = f"/var/lib/clickhouse/fcache{i}/"

    # Add random settings
    if random.randint(1, 100) <= 70:
        apply_properties_recursively(next_cache, cache_storage_properties)


class CachePropertiesGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        # filesystem_caches_config = ET.SubElement(root, "filesystem_caches")
        lower_bound, upper_bound = args.number_caches
        number_caches = random.randint(lower_bound, upper_bound)
        for i in range(0, number_caches):
            add_single_cache(i, ET.SubElement(property_element, f"fcache{i}"))


class KeeperMapPropertiesGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        property_element.text = "/keeper_map_tables"


class TransactionsPropertiesGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        property_element.text = "1"


class DistributedDDLPropertiesGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        path_xml = ET.SubElement(property_element, "path")
        path_xml.text = "/clickhouse/task_queue/ddl"
        replicas_path_xml = ET.SubElement(property_element, "replicas_path")
        replicas_path_xml.text = "/clickhouse/task_queue/replicas"
        apply_properties_recursively(property_element, distributed_properties, 0)


class SharedCatalogPropertiesGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        number_clusters = 0
        shared_settings = {
            "delay_before_drop_intention_seconds": threshold_generator(
                0.2, 0.2, 0, 60, 32
            ),
            "delay_before_drop_table_seconds": threshold_generator(0.2, 0.2, 0, 60, 31),
            "drop_local_thread_pool_size": threads_lambda,
            "drop_ignore_inactive_replica_after_seconds": threshold_generator(
                0.2, 0.2, 0, 60, 32
            ),
            "drop_lock_duration_seconds": threshold_generator(0.2, 0.2, 0, 60, 31),
            "drop_zookeeper_thread_pool_size": threads_lambda,
            # "migration_from_database_replicated": true_false_lambda, not suitable for testing
            "state_application_thread_pool_size": threads_lambda,
        }
        remote_servers = top_root.find("remote_servers")
        if remote_servers is not None:
            number_clusters = len(
                [c for c in remote_servers if "remove" not in c.attrib]
            )
        if number_clusters > 0 and random.randint(1, 100) <= 75:
            cluster_name_choices = [f"cluster{i}" for i in range(0, number_clusters)]
            if remote_servers is None or remote_servers.find("default") is None:
                # The default cluster was not removed
                cluster_name_choices.append("default")
            shared_settings["cluster_name"] = lambda: random.choice(
                cluster_name_choices
            )
        apply_properties_recursively(property_element, shared_settings, 0)


class DatabaseReplicatedGroup(PropertiesGroup):

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        replicated_settings = {
            "allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views": true_false_lambda,
            "check_consistency": true_false_lambda,
            "logs_to_keep": threshold_generator(0.2, 0.2, 0, 3000),
            "max_broken_tables_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
            "max_replication_lag_to_enqueue": threshold_generator(0.2, 0.2, 0, 200),
        }
        apply_properties_recursively(property_element, replicated_settings, 0)


class LogTablePropertiesGroup(PropertiesGroup):

    def __init__(
        self, _log_table: str, _def_max_size_rows: int, _def_reserved_size_rows: int
    ):
        super().__init__()
        self.log_table: str = _log_table
        self.def_max_size_rows: int = _def_max_size_rows
        self.def_reserved_size_rows: int = _def_reserved_size_rows

    def apply_properties(
        self,
        top_root: ET.Element,
        property_element: ET.Element,
        args,
        cluster: ClickHouseCluster,
        is_private_binary: bool,
    ):
        database_xml = ET.SubElement(property_element, "database")
        database_xml.text = "system"
        table_xml = ET.SubElement(property_element, "table")
        table_xml.text = self.log_table

        log_table_properties = {
            "buffer_size_rows_flush_threshold": threshold_generator(0.2, 0.2, 0, 10000),
            "flush_on_crash": true_false_lambda,
            "max_size_rows": threshold_generator(0.2, 0.2, 1, 10000),
            "reserved_size_rows": threshold_generator(0.2, 0.2, 1, 10000),
        }
        # Can't use this without the engine parameter?
        # number_policies = 0
        # storage_configuration_xml = top_root.find("storage_configuration")
        # if storage_configuration_xml is not None:
        #    policies_xml = storage_configuration_xml.find("policies")
        #    if policies_xml is not None:
        #        number_policies = len([c for c in policies_xml])
        # if number_policies > 0 and random.randint(1, 100) <= 75:
        #    policy_choices = [f"policy{i}" for i in range(0, number_policies)]
        #    log_table_properties["storage_policy"] = lambda: random.choice(
        #        policy_choices
        #    )
        apply_properties_recursively(property_element, log_table_properties, 0)
        # max_size_rows (default 1048576) cannot be smaller than reserved_size_rows (default 8192)
        max_size_rows_xml = property_element.find("max_size_rows")
        reserved_size_rows_xml = property_element.find("reserved_size_rows")
        if max_size_rows_xml is not None or reserved_size_rows_xml is not None:
            max_size_rows_value = (
                self.def_max_size_rows
                if (max_size_rows_xml is None or max_size_rows_xml.text is None)
                else int(max_size_rows_xml.text)
            )
            reserved_size_rows_value = (
                self.def_reserved_size_rows
                if (
                    reserved_size_rows_xml is None
                    or reserved_size_rows_xml.text is None
                )
                else int(reserved_size_rows_xml.text)
            )
            if max_size_rows_value < reserved_size_rows_value:
                max_size_rows_xml = (
                    ET.SubElement(property_element, "max_size_rows")
                    if max_size_rows_xml is None
                    else max_size_rows_xml
                )
                max_size_rows_xml.text = str(
                    max(max_size_rows_value, reserved_size_rows_value)
                )


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
    is_private_binary: bool,
    input_config_path: str,
) -> tuple[bool, str, int]:
    modified = False
    number_clusters = 0

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
        https_port_xml = ET.SubElement(root, "https_port")
        https_port_xml.text = "8443"
    if args.with_arrowflight and root.find("arrowflight_port") is None:
        modified = True
        arrowflight_port_xml = ET.SubElement(root, "arrowflight_port")
        arrowflight_port_xml.text = "8888"
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

    if root.find("named_collections") is None:
        modified = True
        named_collections_xml = ET.SubElement(root, "named_collections")
        if args.with_minio:
            s3_xml = ET.SubElement(named_collections_xml, "s3")
            url_xml = ET.SubElement(s3_xml, "url")
            url_xml.text = f"http://{cluster.minio_host}:{cluster.minio_port}/{cluster.minio_bucket}/"
            access_key_id_xml = ET.SubElement(s3_xml, "access_key_id")
            access_key_id_xml.text = "minio"
            secret_access_key_xml = ET.SubElement(s3_xml, "secret_access_key")
            secret_access_key_xml.text = cluster.minio_secret_key
        if args.with_azurite:
            azure_xml = ET.SubElement(named_collections_xml, "azure")
            account_name_xml = ET.SubElement(azure_xml, "account_name")
            account_name_xml.text = cluster.azurite_account
            account_key_xml = ET.SubElement(azure_xml, "account_key")
            account_key_xml.text = cluster.azurite_key
            container_xml = ET.SubElement(azure_xml, "container")
            container_xml.text = cluster.azure_container_name
            storage_account_url_xml = ET.SubElement(azure_xml, "storage_account_url")
            storage_account_url_xml.text = f"http://{cluster.azurite_host}:{cluster.azurite_port}/{cluster.azurite_account}"
        ET.SubElement(named_collections_xml, "local")

    if "timezone" not in possible_properties:
        possible_timezones = get_system_timezones()
        if len(possible_timezones) > 0:
            possible_properties["timezone"] = lambda: random.choice(possible_timezones)
    if "cache_policy" not in cache_storage_properties:
        possible_policies = ["LRU", "SLRU"]
        if is_private_binary:
            possible_policies.extend(["LRU_OVERCOMMIT", "SLRU_OVERCOMMIT"])
        cache_storage_properties["cache_policy"] = lambda: random.choice(
            possible_policies
        )

    selected_properties = {}
    # Select random properties to the XML
    if random.randint(1, 100) <= args.server_settings_prob:
        selected_properties = sample_from_dict(
            possible_properties, random.randint(0, len(possible_properties))
        )

    # Add remote server configurations
    if (
        root.find("remote_servers") is None
        and random.randint(1, 100) <= args.add_remote_server_settings_prob
    ):
        selected_properties["remote_servers"] = ClusterPropertiesGroup()

    # Add disk configurations
    if (
        root.find("storage_configuration") is None
        and root.find("backups") is None
        and root.find("allowed_disks_for_table_engines") is None
        and random.randint(1, 100) <= args.add_disk_settings_prob
    ):
        selected_properties["storage_configuration"] = DiskPropertiesGroup()

    # Add filesystem caches
    if (
        root.find("filesystem_caches") is None
        and random.randint(1, 100) <= args.add_filesystem_caches_prob
    ):
        selected_properties["filesystem_caches"] = CachePropertiesGroup()

    # Add keeper_map_path_prefix
    if args.add_keeper_map_prefix and root.find("keeper_map_path_prefix") is None:
        selected_properties["keeper_map_path_prefix"] = KeeperMapPropertiesGroup()

    # Add experimental transactions
    if args.add_transactions and root.find("allow_experimental_transactions") is None:
        selected_properties["allow_experimental_transactions"] = (
            TransactionsPropertiesGroup()
        )

    # Add distributed_ddl
    if args.add_distributed_ddl and root.find("distributed_ddl") is None:
        selected_properties["distributed_ddl"] = DistributedDDLPropertiesGroup()

    # Add log tables
    if args.add_log_tables:
        all_log_entries = [
            ("asynchronous_insert_log", 1048576, 8192),
            ("asynchronous_metric_log", 1048576, 8192),
            ("backup_log", 1048576, 8192),
            ("blob_storage_log", 1048576, 8192),
            ("crash_log", 1024, 1024),
            ("dead_letter_queue", 1048576, 8192),
            ("delta_lake_metadata_log", 1048576, 8192),
            ("error_log", 1048576, 8192),
            ("iceberg_metadata_log", 1048576, 8192),
            ("metric_log", 1048576, 8192),
            ("opentelemetry_span_log", 1048576, 8192),
            ("part_log", 1048576, 8192),
            ("processors_profile_log", 1048576, 8192),
            ("query_log", 1048576, 8192),
            ("query_metric_log", 1048576, 8192),
            ("query_thread_log", 1048576, 8192),
            ("query_views_log", 1048576, 8192),
            ("session_log", 1048576, 8192),
            ("s3queue_log", 1048576, 8192),
            ("text_log", 1048576, 8192),
            ("trace_log", 1048576, 8192),
            ("zookeeper_connection_log", 1048576, 8192),
            ("zookeeper_log", 1048576, 8192),
        ]
        if random.randint(1, 100) <= 70:
            all_log_entries = random.sample(
                all_log_entries, random.randint(1, len(all_log_entries))
            )
        random.shuffle(all_log_entries)
        for entry in all_log_entries:
            if root.find(entry[0]) is None:
                selected_properties[entry[0]] = LogTablePropertiesGroup(
                    entry[0], entry[1], entry[2]
                )

    # Add shared_database_catalog settings, required for shared catalog to work
    if (
        args.add_shared_catalog
        and is_private_binary
        and root.find("shared_database_catalog") is None
    ):
        selected_properties["shared_database_catalog"] = SharedCatalogPropertiesGroup()

    # Add Replicated databases settings
    if args.add_database_replicated and root.find("database_replicated") is None:
        selected_properties["database_replicated"] = DatabaseReplicatedGroup()

    # Shuffle selected properties and apply
    selected_properties = dict(
        random.sample(list(selected_properties.items()), len(selected_properties))
    )
    for setting, next_child in selected_properties.items():
        if root.find(setting) is None:
            modified = True
            new_element = ET.SubElement(root, setting)
            if isinstance(next_child, dict):
                apply_properties_recursively(new_element, next_child, 0)
            elif isinstance(next_child, PropertiesGroup):
                next_child.apply_properties(
                    root, new_element, args, cluster, is_private_binary
                )
            else:
                new_element.text = str(next_child())

    if modified:
        # Make sure `path` in distributed_ddl is set
        distributed_ddl_xml = root.find("distributed_ddl")
        if distributed_ddl_xml is not None and distributed_ddl_xml.find("path") is None:
            path_xml = ET.SubElement(distributed_ddl_xml, "path")
            path_xml.text = "/clickhouse/task_queue/ddl"
        # Make sure `zookeeper_path` in transaction_log is set
        transaction_log_xml = root.find("transaction_log")
        if (
            transaction_log_xml is not None
            and transaction_log_xml.find("zookeeper_path") is None
        ):
            zookeeper_path_xml = ET.SubElement(transaction_log_xml, "zookeeper_path")
            zookeeper_path_xml.text = "/clickhouse/txn"

    # Get number of clusters if generated, to be used in `users.xml` if needed
    remote_servers = root.find("remote_servers")
    if remote_servers is not None:
        number_clusters = len([c for c in remote_servers if "remove" not in c.attrib])

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


KEEPER_PROPERTIES_TEMPLATE = """
<clickhouse>
    <listen_try>true</listen_try>
    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>

    <logger>
        <level>trace</level>
        <log>/var/log/clickhouse-keeper/clickhouse-keeper.log</log>
        <errorlog>/var/log/clickhouse-keeper/clickhouse-keeper.err.log</errorlog>
    </logger>

    <placement>
        <use_imds>0</use_imds>
        <availability_zone>az-zoo{id}</availability_zone>
    </placement>

    <keeper_server>
        <tcp_port>2181</tcp_port>
        <server_id>{id}</server_id>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>zoo1</hostname>
                <port>9444</port>
            </server>
            <server>
                <id>2</id>
                <hostname>zoo2</hostname>
                <port>9444</port>
            </server>
            <server>
                <id>3</id>
                <hostname>zoo3</hostname>
                <port>9444</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
"""

keeper_settings = {
    "cleanup_old_and_ignore_new_acl": true_false_lambda,
    "coordination_settings": {
        "async_replication": true_false_lambda,
        "auto_forwarding": true_false_lambda,
        "check_node_acl_on_remove": true_false_lambda,
        "commit_logs_cache_size_threshold": threshold_generator(
            0.2, 0.2, 0, 1000 * 1024 * 1024
        ),
        "compress_logs": true_false_lambda,
        "compress_snapshots_with_zstd_format": true_false_lambda,
        "configuration_change_tries_count": threshold_generator(0.2, 0.2, 0, 40),
        "disk_move_retries_during_init": threshold_generator(0.2, 0.2, 0, 200),
        "experimental_use_rocksdb": true_false_lambda,
        "force_sync": true_false_lambda,
        "fresh_log_gap": threshold_generator(0.2, 0.2, 0, 200),
        "latest_logs_cache_size_threshold": threshold_generator(
            0.2, 0.2, 0, 2 * 1024 * 1024 * 1024
        ),
        "log_file_overallocate_size": threshold_generator(
            0.2, 0.2, 0, 100 * 1024 * 1024
        ),
        "max_flush_batch_size": threshold_generator(0.2, 0.2, 0, 2000),
        "max_log_file_size": threshold_generator(0.2, 0.2, 0, 100 * 1024 * 1024),
        "max_request_queue_size": threshold_generator(0.2, 0.2, 0, 100000),
        "max_requests_append_size": threshold_generator(0.2, 0.2, 0, 200),
        "max_requests_batch_bytes_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        "max_requests_batch_size": threshold_generator(0.2, 0.2, 0, 100),
        "max_requests_quick_batch_size": threshold_generator(0.2, 0.2, 0, 200),
        "min_request_size_for_cache": threshold_generator(0.2, 0.2, 0, 100 * 1024),
        "quorum_reads": true_false_lambda,
        "raft_limits_reconnect_limit": threshold_generator(0.2, 0.2, 0, 100),
        "raft_limits_response_limit": threshold_generator(0.2, 0.2, 0, 40),
        "reserved_log_items": threshold_generator(0.2, 0.2, 0, 100000),
        "rocksdb_load_batch_size": threshold_generator(0.2, 0.2, 0, 2000),
        "rotate_log_storage_interval": threshold_generator(0.2, 0.2, 1, 100000),
        "snapshot_distance": threshold_generator(0.2, 0.2, 0, 100000),
        "snapshots_to_keep": threshold_generator(0.2, 0.2, 0, 5),
        "stale_log_gap": threshold_generator(0.2, 0.2, 0, 10000),
        "use_xid_64": true_false_lambda,
    },
    "create_snapshot_on_exit": true_false_lambda,
    "digest_enabled": true_false_lambda,
    "digest_enabled_on_commit": true_false_lambda,
    "enable_reconfiguration": true_false_lambda,
    "feature_flags": {
        "check_not_exists": true_false_lambda,
        "create_if_not_exists": true_false_lambda,
        "filtered_list": true_false_lambda,
        "multi_read": true_false_lambda,
        "multi_watches": true_false_lambda,
        "remove_recursive": true_false_lambda,
    },
    "force_recovery": true_false_lambda,
    "hostname_checks_enabled": true_false_lambda,
    "max_memory_usage_soft_limit": threshold_generator(0.2, 0.2, 0, 1000),
    "max_memory_usage_soft_limit_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "upload_snapshot_on_exit": true_false_lambda,
    **rocksdb_properties,
}


def modify_keeper_settings(args, is_private_binary: bool) -> list[str]:
    result_configs = []
    selected_settings = {}

    if random.randint(1, 100) <= args.keeper_settings_prob:
        selected_settings = sample_from_dict(
            keeper_settings, random.randint(0, len(keeper_settings))
        )

    for i in range(1, 4):
        tree = ET.ElementTree(ET.fromstring(KEEPER_PROPERTIES_TEMPLATE.format(id=i)))
        keeper_server_xml: ET.Element | None = tree.find("keeper_server")

        if keeper_server_xml is not None and len(selected_settings.items()) > 0:
            for setting, next_child in selected_settings.items():
                new_element = ET.SubElement(keeper_server_xml, setting)
                if isinstance(next_child, dict):
                    apply_properties_recursively(new_element, next_child, 0)
                elif isinstance(next_child, PropertiesGroup):
                    raise Exception("Can't use Properties Group here")
                else:
                    new_element.text = str(next_child())

        # Set default coordination settings
        coordination_settings_xml = keeper_server_xml.find("coordination_settings")
        if coordination_settings_xml is None:
            coordination_settings_xml = ET.SubElement(
                keeper_server_xml, "coordination_settings"
            )
        operation_timeout_ms_xml = ET.SubElement(
            coordination_settings_xml, "operation_timeout_ms"
        )
        operation_timeout_ms_xml.text = "10000"
        session_timeout_ms_xml = ET.SubElement(
            coordination_settings_xml, "session_timeout_ms"
        )
        session_timeout_ms_xml.text = "15000"
        raft_logs_level_xml = ET.SubElement(
            coordination_settings_xml, "raft_logs_level"
        )
        raft_logs_level_xml.text = "trace"
        election_timeout_lower_bound_ms_xml = ET.SubElement(
            coordination_settings_xml, "election_timeout_lower_bound_ms"
        )
        election_timeout_lower_bound_ms_xml.text = "2000"
        election_timeout_upper_bound_ms_xml = ET.SubElement(
            coordination_settings_xml, "election_timeout_upper_bound_ms"
        )
        election_timeout_upper_bound_ms_xml.text = "4000"

        # Multi read is required for private binary
        if is_private_binary:
            feature_flags_xml = keeper_server_xml.find("feature_flags")
            if feature_flags_xml is None:
                feature_flags_xml = ET.SubElement(keeper_server_xml, "feature_flags")
            multi_read_xml = feature_flags_xml.find("multi_read")
            if multi_read_xml is None:
                multi_read_xml = ET.SubElement(feature_flags_xml, "multi_read")
            multi_read_xml.text = "1"

        ET.indent(tree, space="    ", level=0)
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as temp_file:
            result_configs.append(temp_file.name)
            tree.write(temp_file.name, encoding="utf-8", xml_declaration=True)
    return result_configs
