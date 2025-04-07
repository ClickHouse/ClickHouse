import os

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()

    Result.from_commands_run(
        name="Fetch latest docs-repo changes",
        command=["git fetch origin main", "git pull"],
        workdir="/opt/clickhouse-docs",
        with_log=True,
        with_info=True,
    )

    Result.from_commands_run(
        name="Install packages",
        command=["yarn install"],
        with_info=True,
        workdir="/opt/clickhouse-docs",
    )

    Result.from_commands_run(
        name="Get ClickHouse/ClickHouse docs",
        command=[f"yarn copy-clickhouse-repo-docs -l {os.getcwd()}"],
        with_info=True,
        workdir="/opt/clickhouse-docs",
    )

    testname = "Markdown lint"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[f"yarn run-markdown-linter"],
            workdir="/opt/clickhouse-docs",
        )
    )

    Result.from_commands_run(
        name=testname,
        command=[f"yarn generate-changelog"],
        workdir="/opt/clickhouse-docs",
    )

    Result.from_commands_run(
        name=testname,
        command=[f"yarn autogenerate-settings"],
        workdir="/opt/clickhouse-docs",
    )

    Result.from_commands_run(
        name=testname,
        command=[f"yarn autogenerate-table-of-contents"],
        workdir="/opt/clickhouse-docs",
    )

    testname = "generate table of contents pages"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[f"yarn autogenerate-table-of-contents"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Build docusaurus"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[
                "yarn build-api-doc",
                "yarn build-swagger",
                "export DOCUSAURUS_IGNORE_SSG_WARNINGS=true && yarn build-docs",
            ],
            workdir="/opt/clickhouse-docs",
        )
    )

    Result.create_from(results=results).complete_job()
