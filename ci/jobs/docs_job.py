from ci.praktika.result import Result
from ci.praktika.utils import Shell

if __name__ == "__main__":

    results = []

    results.append(
        Result.from_commands_run(
            name="task 1", command="echo Hello Shaun", with_info=True
        )
    )

    results.append(
        Result.from_commands_run(
            name="task 2", command="echo How are you?", with_info=True
        )
    )

    results.append(
        Result.from_commands_run(
            name="task 3", command="echo Did you fix the docs job? && exit 1", with_info=True
        )
    )

    Result.create_from(results=results).add_job_summary_to_info(
        with_local_run_command=True
    ).complete_job()
