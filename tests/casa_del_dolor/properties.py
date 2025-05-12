import xml.etree.ElementTree as ET
import tempfile
import multiprocessing
import random
from typing import Dict


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

possible_properties = {
    "allow_use_jemalloc_memory": lambda: random.randint(0, 1),
    "async_insert_queue_flush_on_shutdown": lambda: random.randint(0, 1),
    "async_insert_threads": lambda: random.randint(0, multiprocessing.cpu_count()),
    "async_load_databases": lambda: random.randint(0, 1),
    "async_load_system_database": lambda: random.randint(0, 1),
    "asynchronous_metrics_enable_heavy_metrics": lambda: random.randint(0, 1),
    "background_buffer_flush_schedule_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "background_common_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "background_distributed_schedule_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "background_fetches_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "background_merges_mutations_scheduling_policy": lambda: random.choice(["round_robin", "shortest_task_first"]),
    "background_message_broker_schedule_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "background_move_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    #"background_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()), has to be in a certain range
    "background_schedule_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "backup_threads": lambda: random.randint(0, multiprocessing.cpu_count()),
    "cache_size_to_ram_max_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    #"cannot_allocate_thread_fault_injection_probability": threshold_generator(0.2, 0.2, 0.0, 1.0), the server may not start
    "cgroup_memory_watcher_hard_limit_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "cgroup_memory_watcher_soft_limit_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "compiled_expression_cache_elements_size": threshold_generator(0.2, 0.2, 0, 10000),
    "compiled_expression_cache_size": threshold_generator(0.2, 0.2, 0, 134217728),
    "concurrent_threads_scheduler": lambda: random.choice(["round_robin", "fair_round_robin"]),
    "concurrent_threads_soft_limit_num": lambda: random.randint(0, multiprocessing.cpu_count()),
    "concurrent_threads_soft_limit_ratio_to_cores": lambda: random.randint(0, multiprocessing.cpu_count()),
    "database_catalog_drop_table_concurrency": lambda: random.randint(0, multiprocessing.cpu_count()),
    "database_replicated_allow_detach_permanently": lambda: random.randint(0, 1),
    "dictionaries_lazy_load": lambda: random.randint(0, 1),
    "disable_insertion_and_mutation": lambda: random.randint(0, 1),
    "disable_internal_dns_cache": lambda: random.randint(0, 1),
    "display_secrets_in_show_and_select": lambda: random.randint(0, 1),
    "format_alter_operations_with_parentheses": lambda: random.randint(0, 1),
    "ignore_empty_sql_security_in_create_view_query": lambda: random.randint(0, 1),
    "index_mark_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "index_mark_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "index_uncompressed_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "index_uncompressed_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "load_marks_threadpool_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "mark_cache_prewarm_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "mark_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "mark_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "max_active_parts_loading_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_backups_io_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_build_vector_similarity_index_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_database_replicated_create_table_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_entries_for_hash_table_stats": threshold_generator(0.2, 0.2, 0, 10000),
    "max_fetch_partition_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_io_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_materialized_views_count_for_table": threshold_generator(0.2, 0.2, 0, 8),
    "max_open_files": threshold_generator(0.2, 0.2, 0, 100),
    "max_outdated_parts_loading_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_parts_cleaning_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_prefixes_deserialization_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_server_memory_usage_to_ram_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "max_unexpected_parts_loading_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "memory_worker_correct_memory_tracker": lambda: random.randint(0, 1),
    "merges_mutations_memory_usage_to_ram_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "mlock_executable": lambda: random.randint(0, 1),
    "mmap_cache_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "page_cache_free_memory_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "page_cache_max_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "page_cache_min_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "page_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "parts_killer_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "primary_index_cache_prewarm_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "primary_index_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "primary_index_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "process_query_plan_packet": lambda: random.randint(0, 1),
    "query_condition_cache_size": threshold_generator(0.2, 0.2, 0, 104857600),
    "query_condition_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "remap_executable": lambda: random.randint(0, 1),
    "restore_threads": lambda: random.randint(0, multiprocessing.cpu_count()),
    "shutdown_wait_backups_and_restores": lambda: random.randint(0, 1),
    "shutdown_wait_unfinished_queries": lambda: random.randint(0, 1),
    "storage_metadata_write_full_object_key": lambda: random.randint(0, 1),
    "storage_shared_set_join_use_inner_uuid": lambda: random.randint(0, 1),
    "tables_loader_background_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "tables_loader_foreground_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "uncompressed_cache_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "uncompressed_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "use_minimalistic_part_header_in_zookeeper": lambda: random.randint(0, 1),
    "validate_tcp_client_information": lambda: random.randint(0, 1),
    "vector_similarity_index_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "vector_similarity_index_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "wait_dictionaries_load_at_startup": lambda: random.randint(0, 1)
}


def sample_from_dict(d : Dict, sample : int) -> Dict:
    keys = random.sample(list(d), sample)
    values = [d[k] for k in keys]
    return dict(zip(keys, values))


def modify_server_settings_with_random_properties(input_config_path: str) -> str:
    global possible_properties

    # Parse the existing XML file
    tree = ET.parse(input_config_path)
    root = tree.getroot()
    if root.tag != 'clickhouse':
        raise Exception("<clickhouse> element not found")

    # Select random properties to the XML
    selected_props = sample_from_dict(possible_properties, random.randint(1, len(possible_properties)))
    for setting, generator in selected_props.items():
        if root.find(setting) is None:
            new_element = ET.SubElement(root, setting)
            new_element.text = str(generator())

    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.xml', delete=False) as temp_file:
        temp_path = temp_file.name

    # Write the modified XML to the temporary file
    tree.write(temp_path, encoding='utf-8', xml_declaration=True)

    return temp_path
