#!/usr/bin/env python3
import json
import logging
import random
from pathlib import Path
from .ast_fuzzer_job import finalize_job

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.stack_trace_reader import StackTraceReader
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils


def main():
    logging.basicConfig(level=logging.INFO)
    ch = ClickHouseProc()
    temp_dir = Path(f"{Utils.cwd()}/ci/tmp/")
    workspace_path = temp_dir / "workspace"
    workspace_path.mkdir(parents=True, exist_ok=True)
    fuzzer_log = workspace_path / "fuzzer.log"
    buzz_out = workspace_path / "fuzzer_out.sql"
    buzz_config = workspace_path / "fuzz.json"
    info = Info()
    result = Result.from_fs(name=info.job_name)

    buzz_out.touch()
    buzz_config.touch()
    result.status = Result.Status.SUCCESS

    assert Path(f"{temp_dir}/clickhouse").exists(), "ClickHouse binary not found"

    if result.is_ok():
        # Install ClickHouse as first step
        print("Install ClickHouse")

        def install():
            res = ch.install_fuzzer_config()
            if Info().is_local_run:
                return res
            return res and ch.create_log_export_config()

        result_ = Result.from_commands_run(name="Install ClickHouse", command=install)
        if not result_.is_ok():
            result.results = [result_]
            result.set_status(Result.Status.FAILED)

    if result.is_ok():
        # Start ClickHouse as second step
        print("Start ClickHouse")

        def start():
            res = ch.start_light()
            if Info().is_local_run:
                return res
            return res and ch.start_log_exports(
                check_start_time=Utils.Stopwatch().start_time
            )

        result_ = Result.from_commands_run(
            name="Start ClickHouse", command=start, with_log=True
        )
        if not result_.is_ok():
            result.results = [result_]
            result.set_status(Result.Status.FAILED)

    if result.is_ok():
        # Time to Buzz!!
        print("Buzzing")

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
                [
                    _
                    for _ in disabled_engines[
                        : random.randint(1, len(disabled_engines))
                    ]
                ]
            )

        # Generate configuration file
        # No PARALLEL WITH with slow sanitizers
        min_nested_rows = random.randint(0, 10)
        min_insert_rows = random.randint(1, 100)
        buzz_config = {
            "seed": random.randint(1, 18446744073709551615),
            "max_depth": random.randint(2, 5),
            "max_width": random.randint(2, 5),
            "max_databases": random.randint(2, 5),
            "max_tables": random.randint(3, 10),
            "max_views": random.randint(0, 10),
            "max_dictionaries": random.randint(0, 10),
            "max_columns": random.randint(1, 8),
            "min_nested_rows": min_nested_rows,
            "max_nested_rows": random.randint(min_nested_rows, min_nested_rows + 10),
            "min_insert_rows": min_insert_rows,
            "max_insert_rows": random.randint(min_insert_rows, min_insert_rows + 400),
            "min_string_length": random.randint(0, 100),
            "max_parallel_queries": (
                1
                if (
                    any(x in Info().job_name for x in ["tsan", "asan", "msan"])
                    or random.randint(1, 2) == 1
                )
                else random.randint(1, 5)
            ),
            "max_number_alters": (
                1 if random.randint(1, 2) == 1 else random.randint(1, 4)
            ),
            "fuzz_floating_points": random.choice([True, False]),
            "enable_fault_injection_settings": random.choice([True, False]),
            "enable_force_settings": random.choice([True, False]),
            # Don't compare for correctness yet, false positives maybe
            "use_dump_table_oracle": random.randint(0, 1),
            "test_with_fill": False,  # Creating too many issues
            "compare_success_results": False,  # This can give false positives, so disable it
            "allow_infinite_tables": False,  # Creating too many issues
            "allow_hardcoded_inserts": random.choice([True, False]),
            # These are the error codes that I disallow at the moment
            "disallowed_error_codes": "9,11,13,15,99,100,101,102,108,127,162,165,166,167,168,172,209,230,231,234,235,246,256,257,261,271,272,273,274,275,305,307,521,635,637,638,639,640,641,642,645,647,718,1003",
            "oracle_ignore_error_codes": "1,36,43,47,48,53,59,210,262,321,386,403,467",
            "client_file_path": "/var/lib/clickhouse/user_files",
            "server_file_path": "/var/lib/clickhouse/user_files",
            "log_path": buzz_out,
            "read_log": False,
            "allow_memory_tables": random.choice([True, False]),
            "allow_client_restarts": random.choice([True, False]),
            "max_reconnection_attempts": 3,
            "time_to_sleep_between_reconnects": 5000,
            "keeper_map_path_prefix": "/keeper_map_tables",
            "disabled_types": disabled_types_str,
            "disabled_engines": disabled_engines_str,
            # Make CI logs less verbose
            "truncate_output": True,
            # Don't always run transactions, makes many statements fail
            "allow_transactions": random.randint(1, 5) == 1,
            "remote_servers": ["localhost:9000"],
            "remote_secure_servers": ["localhost:9440"],
            "http_servers": ["localhost:8123"],
            "https_servers": ["localhost:8443"],
            # These settings are slow
            "disallowed_settings": [
                "enable_analyzer",
                "apply_settings_from_server",
                "grace_hash_join_initial_buckets",
                "join_default_strictness",
                "restore_replace_external_dictionary_source_to_null",
                "restore_replace_external_engines_to_null",
                "restore_replace_external_table_functions_to_null",
                "send_logs_level",
                "query_plan_max_limit_for_lazy_materialization",
                "max_download_buffer_size",
                "data_type_default_nullable",
                "enable_producing_buckets_out_of_order_in_aggregation",
                "max_temporary_data_on_disk_size_for_query",
                "max_temporary_data_on_disk_size_for_user",
            ],
            # MergeTree settings to set more often
            "hot_table_settings": [
                "add_minmax_index_for_numeric_columns",
                "add_minmax_index_for_string_columns",
                "allow_coalescing_columns_in_partition_or_order_key",
                "allow_experimental_replacing_merge_with_cleanup",
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
                "min_bytes_for_full_part_storage",
                "min_bytes_for_wide_part",
                "ttl_only_drop_parts",
                "vertical_merge_algorithm_min_bytes_to_activate",
                "use_const_adaptive_granularity",
            ],
        }
        with open(buzz_config, "w") as outfile:
            outfile.write(json.dumps(buzz_config))

        # Allow the fuzzer to run for some time, giving it a grace period of 5m to finish once the time
        # out triggers. After that, it'll send a SIGKILL to the fuzzer to make sure it finishes within
        # a reasonable time.
        run_command = f"timeout --verbose --signal TERM --kill-after=5m --preserve-status 15m clickhouse-client --stacktrace --buzz-house-config={buzz_config}"
        Shell.check(command=run_command, verbose=True, log_file=fuzzer_log)

        finalize_job(workspace_path, temp_dir, result, False)


if __name__ == "__main__":
    main()
