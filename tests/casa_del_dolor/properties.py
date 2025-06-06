import xml.etree.ElementTree as ET
import tempfile
import multiprocessing
import random
import typing
import itertools

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


def file_size_value():
    def gen():
        return str(threshold_generator(0.05, 0.3, 1, 100)()) + random.choice(
            ["ki", "Mi", "Gi"]
        )

    return gen


possible_properties = {
    "allow_use_jemalloc_memory": lambda: random.randint(0, 1),
    "async_insert_queue_flush_on_shutdown": lambda: random.randint(0, 1),
    "async_insert_threads": lambda: random.randint(0, multiprocessing.cpu_count()),
    "async_load_databases": lambda: random.randint(0, 1),
    "async_load_system_database": lambda: random.randint(0, 1),
    "asynchronous_metrics_enable_heavy_metrics": lambda: random.randint(0, 1),
    "background_buffer_flush_schedule_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "background_common_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "background_distributed_schedule_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "background_fetches_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "background_merges_mutations_scheduling_policy": lambda: random.choice(
        ["round_robin", "shortest_task_first"]
    ),
    "background_message_broker_schedule_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "background_move_pool_size": lambda: random.randint(1, multiprocessing.cpu_count()),
    # "background_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()), has to be in a certain range
    "background_schedule_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "backup_threads": lambda: random.randint(1, multiprocessing.cpu_count()),
    "cache_size_to_ram_max_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    # "cannot_allocate_thread_fault_injection_probability": threshold_generator(0.2, 0.2, 0.0, 1.0), the server may not start
    "cgroup_memory_watcher_hard_limit_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "cgroup_memory_watcher_soft_limit_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "compiled_expression_cache_elements_size": threshold_generator(0.2, 0.2, 0, 10000),
    "compiled_expression_cache_size": threshold_generator(0.2, 0.2, 0, 134217728),
    "concurrent_threads_scheduler": lambda: random.choice(
        ["round_robin", "fair_round_robin"]
    ),
    "concurrent_threads_soft_limit_num": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "concurrent_threads_soft_limit_ratio_to_cores": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "database_catalog_drop_table_concurrency": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
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
    "load_marks_threadpool_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "mark_cache_prewarm_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "mark_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "mark_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "max_active_parts_loading_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_backups_io_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_build_vector_similarity_index_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_database_replicated_create_table_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_entries_for_hash_table_stats": threshold_generator(0.2, 0.2, 0, 10000),
    "max_fetch_partition_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_io_thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_materialized_views_count_for_table": threshold_generator(0.2, 0.2, 0, 8),
    "max_open_files": threshold_generator(0.2, 0.2, 0, 100),
    "max_outdated_parts_loading_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_parts_cleaning_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_prefixes_deserialization_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "max_server_memory_usage_to_ram_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "max_unexpected_parts_loading_thread_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "memory_worker_correct_memory_tracker": lambda: random.randint(0, 1),
    "merges_mutations_memory_usage_to_ram_ratio": threshold_generator(
        0.2, 0.2, 0.0, 1.0
    ),
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
    "restore_threads": lambda: random.randint(1, multiprocessing.cpu_count()),
    "shutdown_wait_backups_and_restores": lambda: random.randint(0, 1),
    "shutdown_wait_unfinished_queries": lambda: random.randint(0, 1),
    "storage_metadata_write_full_object_key": lambda: random.randint(0, 1),
    "storage_shared_set_join_use_inner_uuid": lambda: random.randint(0, 1),
    "tables_loader_background_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "tables_loader_foreground_pool_size": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "uncompressed_cache_size": threshold_generator(0.2, 0.2, 0, 2097152),
    "uncompressed_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "use_minimalistic_part_header_in_zookeeper": lambda: random.randint(0, 1),
    "validate_tcp_client_information": lambda: random.randint(0, 1),
    "vector_similarity_index_cache_size": threshold_generator(0.2, 0.2, 0, 5368709120),
    "vector_similarity_index_cache_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "wait_dictionaries_load_at_startup": lambda: random.randint(0, 1),
}


object_storages_properties = {
    "local": {},
    "s3": {
        "min_bytes_for_seek": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024 * 1024),
        "object_metadata_cache_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024 * 1024
        ),
        "s3_check_objects_after_upload": lambda: random.randint(0, 1),
        "s3_max_inflight_parts_for_one_file": threshold_generator(0.2, 0.2, 0, 16),
        "s3_max_get_burst": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_get_rps": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_put_burst": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_put_rps": threshold_generator(0.2, 0.2, 0, 100),
        "s3_max_single_part_upload_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024
        ),
        # "server_side_encryption_customer_key_base64": lambda: random.randint(0, 1), not working well
        "skip_access_check": lambda: random.randint(0, 1),
        "support_batch_delete": lambda: random.randint(0, 1),
        "thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
        "use_insecure_imds_request": lambda: random.randint(0, 1),
    },
    "azure": {
        "max_single_download_retries": threshold_generator(0.2, 0.2, 0, 16),
        "max_single_part_upload_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024 * 1024
        ),
        "max_single_read_retries": threshold_generator(0.2, 0.2, 0, 16),
        "metadata_keep_free_space_bytes": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024 * 1024
        ),
        "min_bytes_for_seek": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024 * 1024),
        "min_upload_part_size": threshold_generator(
            0.2, 0.2, 0, 10 * 1024 * 1024 * 1024
        ),
        "skip_access_check": lambda: random.randint(0, 1),
        "thread_pool_size": lambda: random.randint(0, multiprocessing.cpu_count()),
        "use_native_copy": lambda: random.randint(0, 1),
    },
    "web": {},
}


cache_storage_properties = {
    "allow_dynamic_cache_resize": lambda: random.randint(0, 1),
    "background_download_max_file_segment_size": threshold_generator(
        0.2, 0.2, 0, 10 * 1024 * 1024 * 1024
    ),
    "background_download_queue_size_limit": threshold_generator(0.2, 0.2, 0, 128),
    "background_download_threads": lambda: random.randint(
        0, multiprocessing.cpu_count()
    ),
    "boundary_alignment": threshold_generator(0.2, 0.2, 0, 128),
    "cache_hits_threshold": threshold_generator(0.2, 0.2, 0, 10 * 1024 * 1024 * 1024),
    "cache_on_write_operations": lambda: random.randint(0, 1),
    "cache_policy": lambda: random.choice(["LRU", "SLRU"]),
    "enable_bypass_cache_with_threshold": lambda: random.randint(0, 1),
    "enable_filesystem_query_cache_limit": lambda: random.randint(0, 1),
    "keep_free_space_elements_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "keep_free_space_remove_batch": threshold_generator(
        0.2, 0.2, 0, 10 * 1024 * 1024 * 1024
    ),
    "keep_free_space_size_ratio": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "load_metadata_asynchronously": lambda: random.randint(0, 1),
    "load_metadata_threads": lambda: random.randint(0, multiprocessing.cpu_count()),
    "max_elements": threshold_generator(0.2, 0.2, 1, 10000000),
    "max_file_segment_size": file_size_value(),
    # "max_size_ratio_to_total_space": threshold_generator(0.2, 0.2, 0.0, 1.0), cannot be specified with `max_size` at the same time
    "slru_size_ratio": threshold_generator(0.2, 0.2, 0.01, 0.99),
    "write_cache_per_user_id_directory": lambda: random.randint(0, 1),
}


policy_properties = {
    "load_balancing": lambda: random.choice(["round_robin", "least_used"]),
    "max_data_part_size_bytes": threshold_generator(
        0.2, 0.2, 0, 10 * 1024 * 1024 * 1024
    ),
    "move_factor": threshold_generator(0.2, 0.2, 0.0, 1.0),
    "perform_ttl_move_on_insert": lambda: random.randint(0, 1),
    "prefer_not_to_merge": lambda: random.randint(0, 1),
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
            if is_private_binary:
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
            path_xml.text = f"disk{i}/"
            allowed_path_xml = ET.SubElement(backups_element, "allowed_path")
            allowed_path_xml.text = f"disk{i}/"

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
            path_xml.text = f"disk{i}/"
            allowed_path_xml = ET.SubElement(backups_element, "allowed_path")
            allowed_path_xml.text = f"disk{i}/"

        if disk_type == "cache":
            max_size_xml = ET.SubElement(next_disk, "max_size")
            max_size_xml.text = file_size_value()()

            # Add random settings
            if random.randint(1, 100) <= 70:
                add_settings_from_dict(cache_storage_properties, next_disk)
        else:
            enc_algorithm = random.choice(["aes_128_ctr", "aes_192_ctr", "aes_256_ctr"])
            algorithm_xml = ET.SubElement(next_disk, "algorithm")
            algorithm_xml.text = enc_algorithm

            if enc_algorithm == "aes_128_ctr":
                key_xml = ET.SubElement(next_disk, "key")
                key_xml.text = f"{i % 10}234567812345678"
            else:
                key_hex_xml = ET.SubElement(next_disk, "key_hex")
                key_hex_xml.text = (
                    f"{i % 10}09105c600c12066f82f1a4dbb41a08e4A4348C8387ADB6A"
                    if enc_algorithm == "aes_192_ctr"
                    else f"{i % 10}09105c600c12066f82f1a4dbb41a08e4A4348C8387ADB6AB827410C4EF71CA5"
                )
    return (prev_disk, final_type)


def modify_server_settings(
    args, cluster: ClickHouseCluster, is_private_binary: bool, input_config_path: str
) -> tuple[bool, str]:
    modified = False
    # Parse the existing XML file
    tree = ET.parse(input_config_path)
    root = tree.getroot()
    if root.tag != "clickhouse":
        raise Exception("<clickhouse> element not found")

    # Add disk configurations
    if (
        root.find("storage_configuration") is None
        and random.randint(1, 100) <= args.add_disk_settings_prob
    ):
        modified = True

        storage_config = ET.SubElement(root, "storage_configuration")
        disk_element = ET.SubElement(storage_config, "disks")
        backups_element = ET.SubElement(root, "backups")
        number_disks = random.randint(args.min_disks, args.max_disks)

        allowed_disk_xml = ET.SubElement(backups_element, "allowed_disk")
        allowed_disk_xml.text = "default"
        created_disks_types = []

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
            number_policies = random.randint(args.min_disks, args.max_disks)
            disk_permutations = list(itertools.permutations(bottom_disks))

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
                inputs = random.choice(disk_permutations)
                for i in range(0, number_elements):
                    if main_xml is None or random.randint(1, 3) == 1:
                        if main_xml is not None and random.randint(1, 100) <= 70:
                            add_settings_from_dict(policy_properties, main_xml)
                        main_xml = ET.SubElement(volumes_xml, f"volume{volume_counter}")
                        volume_counter += 1
                    disk_xml = ET.SubElement(main_xml, "disk")
                    disk_xml.text = f"disk{inputs[i]}"
                if main_xml is not None and random.randint(1, 100) <= 70:
                    add_settings_from_dict(policy_properties, main_xml)
                if random.randint(1, 100) <= 70:
                    add_settings_from_dict(policy_properties, next_policy_xml)

        allowed_path_xml1 = ET.SubElement(backups_element, "allowed_path")
        allowed_path_xml1.text = "/var/lib/clickhouse/"
        allowed_path_xml2 = ET.SubElement(backups_element, "allowed_path")
        allowed_path_xml2.text = "/var/lib/clickhouse/user_files/"

    if args.add_keeper_map_prefix:
        modified = True
        new_element = ET.SubElement(root, "keeper_map_path_prefix")
        new_element.text = "/keeper_map_tables"

    # Select random properties to the XML
    if random.randint(1, 100) <= args.server_settings_prob:
        selected_props = sample_from_dict(
            possible_properties, random.randint(1, len(possible_properties))
        )
        for setting, generator in selected_props.items():
            if root.find(setting) is None:
                modified = True
                new_element = ET.SubElement(root, setting)
                new_element.text = str(generator())

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
