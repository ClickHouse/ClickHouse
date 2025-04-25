from ci.praktika.result import Result

if __name__ == "__main__":
    Result.from_gtest_run(
        unit_tests_path="./ci/tmp/unit_tests_dbms",
    ).complete_job()
