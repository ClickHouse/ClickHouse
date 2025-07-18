from ci.jobs.scripts.clickhouse_proc import ClickHouseLight
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

import json, random

temp_dir = f"{Utils.cwd()}/ci/tmp/"


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseLight()

    if res:
        print("Install ClickHouse")

        def install():
            return (
                ch.install()
                and ch.clickbench_config_tweaks()
                and ch.fuzzer_config_tweaks()
                and ch.create_log_export_config()
            )

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=install)
        )
        res = results[-1].is_ok()

    if res:
        print("Start ClickHouse")

        def start():
            return ch.start() and (
                ch.start_log_exports(check_start_time=stop_watch.start_time)
                if not Info().is_local_run
                else True
            )

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=start,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Buzzing")

        log_file = f"{temp_dir}/fuzzer_out.sql"
        buzz_config_file = f"{temp_dir}/fuzz.json"

        # Generate configuration file
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
            "fuzz_floating_points": random.choice([True, False]),
            "enable_fault_injection_settings": random.choice([True, False]),
            "enable_force_settings": random.choice([True, False]),
            # Don't compare for correctness yet, false positives maybe
            "use_dump_table_oracle": random.randint(0, 1),
            "test_with_fill": random.choice([True, False]),
            "compare_success_results": False,  # This can give false positives, so disable it
            "allow_infinite_tables": random.choice([True, False]),
            # These are the error codes that I disallow at the moment
            "disallowed_error_codes": "9,13,15,99,100,101,102,108,127,128,159,162,165,166,173,209,230,231,233,234,235,246,255,256,257,261,270,271,272,273,274,275,305,307,635,637,638,639,640,641,642,645,647,718,1003",
            "client_file_path": "/var/lib/clickhouse/user_files",
            "server_file_path": "/var/lib/clickhouse/user_files",
            "log_path": log_file,
            "read_log": False,
            "allow_memory_tables": random.choice([True, False]),
            "allow_client_restarts": random.choice([True, False]),
            "max_reconnection_attempts": 3,
            "time_to_sleep_between_reconnects": 5000,
            "keeper_map_path_prefix": "/keeper_map_tables",
            # Don't use the old analyzer, it crashes sometimes
            "disable_new_analyzer": False,
        }
        with open(buzz_config_file, "w") as outfile:
            outfile.write(json.dumps(buzz_config))

        results.append(
            Result.from_commands_run(
                name="Buzzing",
                command=[f"clickhouse-client --buzz-house-config={buzz_config_file}"],
            )
        )
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch, files=[]).complete_job()


if __name__ == "__main__":
    main()
