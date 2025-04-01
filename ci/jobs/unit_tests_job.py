from ci.praktika.result import Result

if __name__ == "__main__":
    Result.from_gtest_run(
        name="",
        unit_tests_path="./ci/tmp/unit_tests_dbms",
    ).add_job_summary_to_info(with_local_run_command=True).complete_job()
