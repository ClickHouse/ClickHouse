#!/usr/bin/env python3

import json
import os
import random
from pathlib import Path

from ci.jobs.ast_fuzzer_job import run_fuzz_job
from ci.praktika.utils import Utils


def main():
    temp_dir = Path(f"{Utils.cwd()}/ci/tmp/")
    workspace_path = temp_dir / "workspace"
    buzz_config_file = workspace_path / "fuzz.json"

    workspace_path.mkdir(parents=True, exist_ok=True)

    seed_env = os.getenv("BUZZHOUSE_SEED", "").strip()
    seed = int(seed_env) if seed_env else random.randint(1, 18446744073709551615)

    # Disable all optional engines so we generate only MergeTree-family tables.
    disabled_engines = [
        "replacingmergetree",
        "coalescingmergetree",
        "summingmergetree",
        "aggregatingmergetree",
        "collapsingmergetree",
        "versionedcollapsingmergetree",
        "file",
        "null",
        "set",
        "join",
        "memory",
        "stripelog",
        "log",
        "tinylog",
        "embeddedrocksdb",
        "buffer",
        "mysql",
        "postgresql",
        "sqlite",
        "mongodb",
        "redis",
        "s3",
        "s3queue",
        "hudi",
        "deltalakes3",
        "deltalakeazure",
        "deltalakelocal",
        "icebergs3",
        "icebergazure",
        "iceberglocal",
        "merge",
        "distributed",
        "dictionary",
        "generaterandom",
        "azureblobstorage",
        "azurequeue",
        "url",
        "keepermap",
        "externaldistributed",
        "materializedpostgresql",
        "replicated",
        "shared",
        "datalakecatalog",
        "arrowflight",
        "alias",
    ]

    buzz_config = {
        "seed": seed,
        "max_depth": 1,
        "max_width": 1,
        "max_databases": 1,
        "max_tables": 10,
        "max_views": 0,
        "max_dictionaries": 0,
        "max_columns": 5,
        "min_nested_rows": 0,
        "max_nested_rows": 3,
        "min_insert_rows": 10,
        "max_insert_rows": 100,
        "min_string_length": 0,
        "max_string_length": 10,
        "max_parallel_queries": 1,
        "max_number_alters": 1,
        "time_to_run": 300,
        # Compare results of the same query under different settings.
        "allow_query_oracles": True,
        "compare_success_results": True,
        "settings_oracle_only": True,
        "settings_oracle_settings": ["use_primary_key_indexes"],
        "use_dump_table_oracle": 0,
        "test_with_fill": False,
        "allow_infinite_tables": False,
        "allow_health_check": False,
        "allow_client_restarts": False,
        "enable_fault_injection_settings": False,
        "enable_force_settings": False,
        "enable_compatibility_settings": False,
        "allow_hardcoded_inserts": False,
        "client_file_path": "/workspace/user_files",
        "server_file_path": "/workspace/user_files",
        "log_path": "/workspace/fuzzerout.sql",
        "read_log": False,
        "allow_memory_tables": False,
        "random_limited_values": False,
        "max_reconnection_attempts": 3,
        "time_to_sleep_between_reconnects": 5000,
        "keeper_map_path_prefix": "/keeper_map_tables",
        "deterministic_prob": 100,
        "disabled_engines": ",".join(disabled_engines),
        "truncate_output": True,
        "allow_transactions": False,
        "remote_servers": ["localhost:9000"],
        "remote_secure_servers": ["localhost:9440"],
        "http_servers": ["localhost:8123"],
        "https_servers": ["localhost:8443"],
        # Prefer MergeTree settings that affect marks/ranges.
        "hot_table_settings": [
            "add_minmax_index_for_numeric_columns",
            "add_minmax_index_for_string_columns",
            "allow_experimental_reverse_key",
            "allow_floating_point_partition_key",
            "allow_nullable_key",
            "allow_suspicious_indices",
            "enable_mixed_granularity_parts",
            "index_granularity",
            "index_granularity_bytes",
            "merge_max_block_size",
            "min_bytes_for_wide_part",
            "min_rows_for_wide_part",
            "use_const_adaptive_granularity",
        ],
        "disallowed_settings": [
            # Disable old analyzer always.
            "enable_analyzer",
            # Don't always apply settings from the server.
            "apply_settings_from_server",
            # Keep all debugging messages.
            "send_logs_level",
            # Slow settings.
            "enable_producing_buckets_out_of_order_in_aggregation",
            "grace_hash_join_initial_buckets",
            "join_default_strictness",
            "max_download_buffer_size",
            "max_partitions_per_insert_block",
            "max_table_size_to_drop",
            "max_temporary_data_on_disk_size_for_query",
            "max_temporary_data_on_disk_size_for_user",
            "max_untracked_memory",
            # Makes everything nullable, not handy.
            "data_type_default_nullable",
            "restore_replace_external_dictionary_source_to_null",
            "restore_replace_external_engines_to_null",
            "restore_replace_external_table_functions_to_null",
            # Not handy.
            "insert_shard_id",
            "union_default_mode",
            "except_default_mode",
            "input_format_skip_unknown_fields",
            "unknown_packet_in_send_data",
        ],
    }

    with open(buzz_config_file, "w", encoding="utf-8") as outfile:
        outfile.write(json.dumps(buzz_config))

    run_fuzz_job("BuzzHouse use_primary_key_indexes")


if __name__ == "__main__":
    main()
