from ci.jobs.scripts.clickhouse_proc import ClickHouseLight
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

from pathlib import Path
import json, random

temp_dir = f"{Utils.cwd()}/ci/tmp/"


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseLight()
    shell_log_file = f"{temp_dir}/buzzing.log"
    buzz_log_file = f"{temp_dir}/fuzzer_out.sql"
    buzz_config_file = f"{temp_dir}/fuzz.json"

    Path(buzz_log_file).touch()
    Path(buzz_config_file).touch()

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

        # Sometimes disallow SQL types to reduce number of combinations
        disabled_types_str = ""
        if random.randint(1, 2) == 1:
            disabled_types = [
                "bool",
                "uint",
                "int8",
                "int64",
                "int128",
                "float",
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
            "disallowed_error_codes": "9,13,15,99,100,101,102,108,127,162,165,166,173,209,230,231,233,234,235,246,256,257,261,270,271,272,273,274,275,305,307,635,637,638,639,640,642,645,647,718,1003",
            "client_file_path": "/var/lib/clickhouse/user_files",
            "server_file_path": "/var/lib/clickhouse/user_files",
            "log_path": buzz_log_file,
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
        }
        with open(buzz_config_file, "w") as outfile:
            outfile.write(json.dumps(buzz_config))

        # Allow the fuzzer to run for some time, giving it a grace period of 5m to finish once the time
        # out triggers. After that, it'll send a SIGKILL to the fuzzer to make sure it finishes within
        # a reasonable time.
        run = Shell.run(
            command=[
                f"timeout --verbose --signal TERM --kill-after=5m --preserve-status 30m clickhouse-client --stacktrace --buzz-house-config={buzz_config_file}"
            ],
            verbose=True,
            log_file=shell_log_file,
        )

        # On SIGTERM due to timeout, exit code is 143, ignore it
        results.append(
            Result.create_from(
                name="Buzzing result",
                status=(
                    Result.Status.SUCCESS if run in (0, 143) else Result.Status.FAILED
                ),
                info=Shell.get_output(f"tail -300 {shell_log_file}")
            )
        )
        res = results[-1].is_ok()

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=[shell_log_file, buzz_config_file, buzz_log_file],
    ).complete_job()


if __name__ == "__main__":
    main()
