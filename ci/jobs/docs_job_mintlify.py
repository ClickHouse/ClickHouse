import os

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"

    results.append(
        Result.from_commands_run(
            name="Verify Mintlify docs.json file is valid",
            command=[
                "mint validate",
            ]
        )
    )

    results.append(
        Result.from_commands_run(
            name="Check for broken links",
            command=[
                "mint broken-links",
            ]
        )
    )

    Result.create_from(results=results).complete_job()
