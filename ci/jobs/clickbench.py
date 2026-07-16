import os
import shutil
import sys
import traceback
from pathlib import Path

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


def install_introspection(config_dir, var_lib_dir):
    users_d = f"{config_dir}/users.d"
    os.makedirs(users_d, exist_ok=True)
    content = """
profiles:
    default:
        allow_introspection_functions: 1
"""
    file_path = f"{users_d}/allow_introspection_functions.yaml"
    with open(file_path, "w") as file:
        file.write(content)
    return True


def install_clickbench_cache(config_dir, var_lib_dir):
    config_d = f"{config_dir}/config.d"
    # `programs/server/config.d/storage_conf_local.xml` is a symlink to the
    # test-only config that defines pre-configured `local_cache*` disks with
    # `max_size = 22548578304` (~21 GiB) under relative path `local_cache/`.
    # With our `filesystem_caches_path` override the cache base path becomes
    # `/dev/shm/clickhouse/local_cache/`, but the ClickBench container runs with
    # `--shm-size=16g`, so the capacity check in `FileCache::initialize` rejects
    # the disk and the server fails to start. ClickBench doesn't use these test
    # disks, so drop the stock file and install our own cache-path override.
    Path(f"{config_d}/storage_conf_local.xml").unlink(missing_ok=True)
    shutil.copy(
        "./ci/jobs/scripts/clickbench/filesystem_caches_path.xml", config_d
    )
    return True


def install_ci_logs_sender(config_dir, var_lib_dir):
    # `setup_log_cluster.sh --setup-logs-replication` creates the `*_watcher`
    # materialized views with `DEFINER = ci_logs_sender`, so that user must
    # exist on the server, or replication setup fails with `Code: 192. There is
    # no user 'ci_logs_sender'`. The base server config does not ship it, so
    # install the same definition the functional-test config uses.
    users_d = Path(config_dir) / "users.d"
    users_d.mkdir(parents=True, exist_ok=True)
    shutil.copy("./tests/config/users.d/ci_logs_sender.yaml", users_d)
    return True


def main():
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseProc(ch_config_dir=f"{temp_dir}/etc/clickhouse-server")
    info = Info()

    try:
        config_hooks = [
            ClickHouseService.install_base,
            install_introspection,
            install_clickbench_cache,
        ]
        if not info.is_local_run:
            config_hooks.append(install_ci_logs_sender)
            config_hooks.append(
                lambda config_dir, var_lib_dir: ch.create_log_export_config(
                    config_dir
                )
            )

        with ClickHouseService(
            results=results,
            config_hooks=config_hooks,
        ):
            if not info.is_local_run:
                if not ch.start_log_exports(check_start_time=stop_watch.start_time):
                    print("WARNING: Failed to start log export")

            results.append(
                r := Result.from_commands_run(
                    name="Load the data",
                    command="clickhouse-client --time < ./ci/jobs/scripts/clickbench/create.sql",
                )
            )
            if not r.is_ok():
                raise RuntimeError(f"[{r.name}] failed: {r.info}")

            print("Queries")
            stop_watch_ = Utils.Stopwatch()
            TRIES = 3
            QUERY_NUM = 1

            with open(
                "./ci/jobs/scripts/clickbench/queries.sql", "r"
            ) as queries_file:
                query_results = []
                for query in queries_file:
                    query = query.strip()
                    timing = []

                    for i in range(1, TRIES + 1):
                        query_id = f"q{QUERY_NUM}-{i}"
                        res, out, time_err = Shell.get_res_stdout_stderr(
                            f'clickhouse-client --query_id {query_id} --time --format Null --query "{query}" --progress 0',
                            verbose=True,
                        )
                        timing.append(time_err)
                        query_results.append(
                            Result(
                                name=f"{QUERY_NUM}_{i}",
                                status=Result.Status.OK,
                                duration=float(time_err),
                            )
                        )
                    print(timing)
                    QUERY_NUM += 1

            results.append(
                Result.create_from(
                    name="Queries", results=query_results, stopwatch=stop_watch_
                )
            )
    except Exception as e:
        print(traceback.format_exc(), file=sys.stdout)
        results.append(
            Result(name="Job error", status=Result.Status.FAIL, info=str(e))
        )

    # stop log replication
    Shell.check(
        "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
        verbose=True,
    )

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=ch.prepare_logs(all=False, info=info),
    ).complete_job()


if __name__ == "__main__":
    main()
