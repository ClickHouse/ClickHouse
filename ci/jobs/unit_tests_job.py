from ci.praktika.info import Info
from ci.praktika.result import Result

if __name__ == "__main__":
    # Note, LSan does not compatible with debugger
    if "asan" not in Info().job_name:
        # With gdb we will capture stacktrace in case of abnormal termination and timeout (45 mins)
        command_launcher = f"timeout -v 45m gdb -batch -ex 'handle all nostop' -ex 'set print thread-events off' -ex run -ex bt -ex 'thread apply all bt' -arg"
    else:
        command_launcher = ""

    Result.from_gtest_run(
        unit_tests_path="./ci/tmp/unit_tests_dbms",
        command_launcher=command_launcher,
    ).complete_job()
