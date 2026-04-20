import argparse
import shlex

from ci.jobs.integration_test_job import start_docker_in_docker
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika.result import Result
from ci.praktika.utils import Utils

temp_path = f"{Utils.cwd()}/ci/tmp"


def parse_args():
    parser = argparse.ArgumentParser(description="CI Tests job")
    parser.add_argument(
        "--test",
        help="Optional. Test name patterns passed to pytest -k (space-separated)",
        default=[],
        nargs="+",
        action="extend",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    pytest_command = "ci/tests/"
    if args.test:
        pytest_command += " -k " + shlex.quote(" or ".join(args.test))

    start_docker_in_docker()
    with ClickHouseService() as service:
        test_result = Result.from_pytest_run(
            name="CI Tests",
            command=pytest_command,
            pytest_report_file=f"{temp_path}/pytest_ci_tests.jsonl",
            pytest_logfile=f"{temp_path}/pytest_ci_tests.log",
            logfile=f"{temp_path}/ci_tests.log",
            timeout=600,
        )

    test_result.complete_job()
