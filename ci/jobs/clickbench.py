from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseProc()
    info = Info()

    if res:
        print("Install ClickHouse")

        def install():
            res = ch.install_clickbench_config()
            if info.is_local_run:
                return res
            return res and ch.create_log_export_config()

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=install)
        )
        res = results[-1].is_ok()

    if res:
        print("Start ClickHouse")

        def start():
            res = ch.start_light()
            if info.is_local_run:
                return res
            return res and ch.start_log_exports(check_start_time=stop_watch.start_time)

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=start,
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Load the data")
        results.append(
            Result.from_commands_run(
                name="Load the data",
                command="clickhouse-client --time < ./ci/jobs/scripts/clickbench/create.sql",
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Queries")
        stop_watch_ = Utils.Stopwatch()
        TRIES = 3
        QUERY_NUM = 1

        with open("./ci/jobs/scripts/clickbench/queries.sql", "r") as queries_file:
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
                            status=Result.Status.SUCCESS,
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
        res = results[-1].is_ok()

    # stop log replication
    Shell.check(
        f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
        verbose=True,
    )

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=ch.prepare_logs(all=False, info=info),
    ).complete_job()


if __name__ == "__main__":
    main()
