import os

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"

    docs_dir = f"{Utils.cwd()}/docs"

    results.append(
        Result.from_commands_run(
            name="Verify Mintlify docs.json file is valid",
            command=[
                "mint validate",
            ],
            workdir=docs_dir,
        )
    )

    results.append(
        Result.from_commands_run(
            name="Check for broken links",
            command=[
                "mint broken-links",
            ],
            workdir=docs_dir,
        )
    )

    Result.create_from(results=results).complete_job()
