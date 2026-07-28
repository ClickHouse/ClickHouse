import json
import sys
from datetime import datetime

from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# Job collects median per-test-case duration for every CI job, using only
# master CI data (pull_request_number = 0 AND head_ref = 'master').
#
# The output is a JSON mapping of job name -> list of {test_name: median_duration_ms}:
#   {
#     "Stateless tests (amd_asan_ubsan, distributed plan, parallel)": [
#       {"00001_select_1": 120},
#       {"00002_system_numbers": 340},
#       ...
#     ],
#     ...
#   }
#
# The trailing batch suffix ", x/y" (e.g. ", 1/2") is stripped from the job name,
# so all batches of the same job are merged into a single entry.

DAYS = 2

TEST_STATISTICS_QUERY = """
SELECT
    replaceRegexpOne(check_name, ',\\\\s*[0-9]+/[0-9]+\\\\)', ')') AS job,
    test_name,
    toUInt64(round(median(test_duration_ms))) AS median_ms
FROM default.checks
WHERE
    check_start_time >= now() - INTERVAL {DAYS} DAY
    AND pull_request_number = 0
    AND head_ref = 'master'
    AND test_name != ''
    AND test_status = 'OK'
GROUP BY job, test_name
ORDER BY job, test_name
FORMAT JSONEachRow
"""


if __name__ == "__main__":

    days = int(sys.argv[1]) if len(sys.argv) > 1 else DAYS

    info = Info()
    url_secret = info.get_secret(Settings.SECRET_CI_DB_URL)
    user_secret = info.get_secret(Settings.SECRET_CI_DB_USER)
    passwd_secret = info.get_secret(Settings.SECRET_CI_DB_PASSWORD)
    url, user, pwd = (
        url_secret.join_with(user_secret).join_with(passwd_secret).get_value()
    )
    cidb = CIDB(url=url, user=user, passwd=pwd)

    results = []
    statistics = {}

    print("--- Collect median per-test statistics ---")

    def collect():
        query = TEST_STATISTICS_QUERY.format(DAYS=days)
        output = cidb.query(query)
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            statistics.setdefault(row["job"], []).append(
                {row["test_name"]: int(row["median_ms"])}
            )
        assert statistics, "No statistics collected"
        return True

    results.append(
        Result.from_commands_run(name="Fetch per-test statistics", command=collect)
    )

    print("--- Upload statistics ---")
    statistics_link = None
    if results[-1].is_ok():

        def do():
            global statistics_link
            file_name = "./ci/tmp/test_statistics.json"
            archive_name = "./ci/tmp/test_statistics.json.gz"
            archive_name_with_date = f"./ci/tmp/test_statistics_{datetime.now().strftime('%d_%m_%Y')}.json.gz"

            with open(file_name, "w") as f:
                json.dump(statistics, f, indent=2)

            Shell.check(
                f"rm -f {archive_name} {archive_name_with_date} && gzip -k {file_name} && cp {archive_name} {archive_name_with_date}"
            )
            _ = S3.copy_file_to_s3(
                local_path=archive_name,
                s3_path=f"{Settings.S3_REPORT_BUCKET}/statistics",
                content_type="application/json",
                content_encoding="gzip",
            )
            statistics_link = S3.copy_file_to_s3(
                local_path=archive_name_with_date,
                s3_path=f"{Settings.S3_REPORT_BUCKET}/statistics",
                content_type="application/json",
                content_encoding="gzip",
            )

        results.append(Result.from_commands_run("Upload", command=do))

    Result.create_from(
        results=results, links=[statistics_link] if statistics_link else []
    ).complete_job(with_job_summary_in_info=False)
