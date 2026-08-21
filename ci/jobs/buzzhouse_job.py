#!/usr/bin/env python3
import json
import random
from pathlib import Path

from ci.jobs.ast_fuzzer_job import run_fuzz_job
from ci.jobs.ci_utils import is_extended_run
from ci.praktika.utils import Utils


def main():
    temp_dir = Path(f"{Utils.cwd()}/ci/tmp/")
    workspace_path = temp_dir / "workspace"
    buzz_config_file = workspace_path / "fuzz.json"

    workspace_path.mkdir(parents=True, exist_ok=True)

    # Sometimes disallow SQL types to reduce number of combinations
    disabled_types_str = ""
    if random.randint(1, 2) == 1:
        disabled_types = [
            "bool",
            "uint",
            "int8",
            "int16",
            "int64",
            "int128",
            "bfloat16",
            "float32",
            "float64",
            "date",
            "date32",
            "time",
            "time64",
            "datetime",
            "datetime64",
            "string",
            "decimal",
            "uuid",
            "enum",
            "dynamic",
            "json",
            "nullable",
            "lcard",
            "array",
            "map",
            "tuple",
            "variant",
            "nested",
            "ipv4",
            "ipv6",
            "geo",
            "fixedstring",
            "qbit",
            "aggregate",
            "simpleaggregate",
        ]
        random.shuffle(disabled_types)
        disabled_types_str = ",".join(
            [_ for _ in disabled_types[: random.randint(1, len(disabled_types))]]
        )

    # Sometimes disallow Table and Database engines to reduce number of combinations
    disabled_engines_str = ""
    if random.randint(1, 2) == 1:
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
        random.shuffle(disabled_engines)
        disabled_engines_str = ",".join(
            [_ for _ in disabled_engines[: random.randint(1, len(disabled_engines))]]
        )

    # Generate configuration file
    # No PARALLEL WITH with slow sanitizers
    # If hardcoded inserts are allowed, reduce insert size, so logs don't grow as much

    allow_transactions = random.randint(1, 5) == 1
    disallowed_settings = [
        # Disable old analyzer always
        "enable_analyzer",
        # Don't always apply settings from the server
        "apply_settings_from_server",
        # Keep all debugging messages
        "send_logs_level",
        # Slow settings
        "enable_producing_buckets_out_of_order_in_aggregation",
        "grace_hash_join_initial_buckets",
        "join_default_strictness",
        "max_download_buffer_size",
        "max_partitions_per_insert_block",
        "max_table_size_to_drop",
        "max_temporary_data_on_disk_size_for_query",
        "max_temporary_data_on_disk_size_for_user",
        "max_untracked_memory",
        # Makes everything nullable, not handy
        "data_type_default_nullable",
        "restore_replace_external_dictionary_source_to_null",
        "restore_replace_external_engines_to_null",
        "restore_replace_external_table_functions_to_null",
        # Not handy
        "default_compression_codec",
        "insert_shard_id",
        "union_default_mode",
        "except_default_mode",
        "input_format_skip_unknown_fields",
        "insert_quorum",
        "temporary_files_buffer_size",
        "trace_profile_events",
        "unknown_packet_in_send_data",
    ]
    if allow_transactions:
        # Doesn't work well with transactions
        disallowed_settings.append("apply_mutations_on_fly")

    allow_hardcoded_inserts = random.choice([True, False])
    min_nested_rows = random.randint(0, 5)
    max_nested_rows = min_nested_rows + (5 if allow_hardcoded_inserts else 100)
    min_insert_rows = random.randint(1, 100)
    max_insert_rows = min_insert_rows + (10 if allow_hardcoded_inserts else 3000)
    min_string_length = random.randint(0, 100)
    max_string_length = min_string_length + (10 if allow_hardcoded_inserts else 300)
    buzz_config = {
        "seed": random.randint(1, 18446744073709551615),
        "max_depth": random.randint(2, 5),
        "max_width": random.randint(2, 7),
        "max_databases": random.randint(2, 5),
        "max_tables": random.randint(3, 10),
        "max_views": random.randint(0, 10),
        "max_dictionaries": random.randint(0, 10),
        "max_columns": random.randint(1, 8),
        "min_nested_rows": min_nested_rows,
        "max_nested_rows": random.randint(min_nested_rows, max_nested_rows),
        "min_insert_rows": min_insert_rows,
        "max_insert_rows": random.randint(min_insert_rows, max_insert_rows),
        "min_string_length": min_string_length,
        "max_string_length": random.randint(min_string_length, max_string_length),
        # Disable parallel queries, there are issues in the CI currently
        # "max_parallel_queries": (
        #    1
        #    if (
        #        any(x in Info().job_name for x in ["tsan", "asan", "msan"])
        #        or random.randint(1, 2) == 1
        #    )
        #    else random.randint(1, 5)
        # ),
        "max_parallel_queries": 1,
        "max_number_alters": (1 if random.randint(1, 2) == 1 else random.randint(1, 4)),
        "fuzz_floating_points": random.choice([True, False]),
        "enable_fault_injection_settings": random.randint(1, 4) == 1,
        "enable_force_settings": random.randint(1, 4) == 1,
        # Don't compare for correctness yet, false positives maybe
        "use_dump_table_oracle": (1 if random.randint(1, 3) == 1 else 0),
        "test_with_fill": False,  # Creating too many issues
        "compare_success_results": False,  # This can give false positives, so disable it
        "allow_infinite_tables": False,  # Creating too many issues
        "allow_health_check": False,  # I have to test this first
        "enable_compatibility_settings": random.randint(1, 4) == 1,
        "enable_memory_settings": random.randint(1, 4) == 1,
        "enable_backups": random.randint(1, 4) == 1,
        "enable_renames": random.randint(1, 4) == 1,
        "allow_hardcoded_inserts": allow_hardcoded_inserts,
        "client_file_path": "/var/lib/clickhouse/user_files",
        "server_file_path": "/var/lib/clickhouse/user_files",
        "log_path": "/workspace/fuzzerout.sql",
        "read_log": False,
        "allow_memory_tables": random.choice([True, False]),
        "allow_client_restarts": random.choice([True, False]),
        # Sometimes use a small range of integer values
        "random_limited_values": random.choice([True, False]),
        "max_reconnection_attempts": 3,
        "time_to_sleep_between_reconnects": 5000,
        "keeper_map_path_prefix": "/keeper_map_tables",
        # Oracles don't run check correctness on CI, so set deterministic probability low
        "deterministic_prob": random.randint(0, 20),
        "disabled_types": disabled_types_str,
        "disabled_engines": disabled_engines_str,
        # Make CI logs less verbose
        "truncate_output": True,
        # Don't always run transactions, makes many statements fail
        "allow_transactions": allow_transactions,
        # Run query oracles sometimes
        "allow_query_oracles": random.randint(1, 4) == 1,
        # Run for 30 minutes by default, or 60 minutes for feature/improvement/performance PRs
        "time_to_run": 60 if is_extended_run() else 30,
        "remote_servers": ["localhost:9000"],
        "remote_secure_servers": ["localhost:9440"],
        "http_servers": ["localhost:8123"],
        "https_servers": ["localhost:8443"],
        "disallowed_settings": disallowed_settings,
        # MergeTree settings to set more often
        "hot_table_settings": [
            "allow_coalescing_columns_in_partition_or_order_key",
            # "allow_experimental_replacing_merge_with_cleanup",
            "allow_experimental_reverse_key",
            "allow_floating_point_partition_key",
            "allow_nullable_key",
            "allow_summing_columns_in_partition_or_order_key",
            "allow_suspicious_indices",
            "allow_vertical_merges_from_compact_to_wide_parts",
            "enable_block_number_column",
            "enable_block_offset_column",
            "enable_vertical_merge_algorithm",
            "index_granularity",
            "merge_max_block_size",
            "min_bytes_for_full_part_storage",
            "min_bytes_for_wide_part",
            "nullable_serialization_version",
            "ratio_of_defaults_for_sparse_serialization",
            "string_serialization_version",
            "vertical_merge_algorithm_min_bytes_to_activate",
        ],
    }
    with open(buzz_config_file, "w") as outfile:
        outfile.write(json.dumps(buzz_config))

    run_fuzz_job("BuzzHouse")


if __name__ == "__main__":
    main()
