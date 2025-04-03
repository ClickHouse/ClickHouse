from ci.praktika.result import Result
from ci.praktika.utils import Shell

if __name__ == "__main__":

    res = Shell.check("echo Hello Shaun && exit 1", verbose=True)

    Result.create_from(status=res, info="Shaun, please fix me").add_job_summary_to_info(
        with_local_run_command=True
    ).complete_job()
