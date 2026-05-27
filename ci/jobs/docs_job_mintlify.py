import os

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"

    testname = "Verify Mintlify build"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=[
                "mint validate",
            ]
        )
    )

    Result.create_from(results=results).complete_job()
