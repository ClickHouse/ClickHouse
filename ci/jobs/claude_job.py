from ci.praktika.result import Result
from ci.praktika.utils import Utils, Shell


def generate_description_changelog_entry():
    res, _, _ = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/claude_scripts/gen_description_changelog.py"
    )
    return res


if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"

    testname = "Autogenerate PR description and changelog"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=generate_description_changelog_entry,
        )
    )

    Result.create_from(results=results).complete_job()
