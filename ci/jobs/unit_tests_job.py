from ci.praktika.result import Result

if __name__ == "__main__":
    # With gdb we will capture stacktrace in case of abnormal termination and timeout (30 mins)
    # 'catch syscall exit_group' is a hack to avoid exiting with non-zero exit code when process terminated successfully
    #
    # Note, LSan does not compatible with debugger, so let's not run binary under gdb for sanitizers
    if "san" not in Info().job_name:
        command_prefix = f"timeout 30m gdb -batch -ex 'handle all nostop' -ex 'set print thread-events off' -ex 'set pagination off' -ex 'catch syscall exit_group' -ex run -ex bt -arg"
    else:
        command_prefix = ""

    Result.from_gtest_run(
        unit_tests_path="./ci/tmp/unit_tests_dbms",
        command_prefix=command_prefix,
    ).complete_job()
