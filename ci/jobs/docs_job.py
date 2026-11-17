import os

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"

    testname = "Fetch latest ClickHouse/clickhouse-docs changes"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=["git clone https://github.com/ClickHouse/clickhouse-docs.git"],
            workdir="/opt",
        )
    )

    testname = "Install required packages"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=["yarn install"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Get ClickHouse/ClickHouse docs"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[f"yarn copy-clickhouse-repo-docs -l {os.getcwd()}"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Run markdown linter"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=["yarn check-markdown"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Generate changelog"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=["yarn generate-changelog"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Generate documentation from source"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[f"yarn autogenerate-settings -b {temp_dir}clickhouse"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Generate table of contents pages"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=["yarn autogenerate-table-of-contents"],
            workdir="/opt/clickhouse-docs",
        )
    )

    # The /opt/clickhouse-docs is a git directory owned by user 999
    # We must add it to the trusted directories to avoid git warnings during the build
    Shell.check(
        "git config --global --add safe.directory /opt/clickhouse-docs", strict=True
    )

    testname = "Build docusaurus"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[
                "export DOCUSAURUS_IGNORE_SSG_WARNINGS=true && yarn build-docs",
            ],
            workdir="/opt/clickhouse-docs",
        )
    )

    Result.create_from(results=results).complete_job()
