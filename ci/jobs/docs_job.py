import os

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()

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
            command=[f"yarn run-markdown-linter"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Generate changelog"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[f"yarn generate-changelog"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Generate documentation from source"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[f"yarn autogenerate-settings"],
            workdir="/opt/clickhouse-docs",
        )
    )

    testname = "Generate table of contents pages"
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
