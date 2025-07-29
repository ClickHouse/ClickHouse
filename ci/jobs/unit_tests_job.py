from ci.praktika.info import Info
from ci.praktika.result import Result

if __name__ == "__main__":
    # With gdb we will capture stacktrace in case of abnormal termination and timeout (45 mins)
    # 'catch syscall 231' (exit_group) is a hack to avoid exiting with non-zero exit code when process terminated successfully
    #
    # Note, LSan does not compatible with debugger
    if "asan" not in Info().job_name:
        command_launcher = f"timeout -v 45m gdb -batch -ex 'handle all nostop' -ex 'set print thread-events off' -ex 'catch syscall 231' -ex run -ex bt -ex 'thread apply all bt' -arg"
    else:
        command_launcher = ""

    Result.from_gtest_run(
        unit_tests_path="./ci/tmp/unit_tests_dbms",
        command_launcher=command_launcher,
    ).complete_job()
