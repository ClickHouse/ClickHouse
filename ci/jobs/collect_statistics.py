import json
import sys
from datetime import datetime

from ci.praktika import Secret
from ci.praktika.cidb import CIDB
from ci.praktika.result import Result
from ci.praktika.s3 import S3
from ci.praktika.utils import Shell
from ci.settings.settings import S3_REPORT_BUCKET_NAME

# Job collects overall CI statistics per each job

# TODO: Should work for generic CI and become native praktika job


QUANTILES = [
    0,
    5,
    10,
    15,
    20,
    25,
    35,
    40,
    45,
    50,
    55,
    60,
    65,
    70,
    75,
    80,
    85,
    90,
    95,
    100,
]

DAYS = [1, 3]

JOB_STATISTICS_QUERY = """
SELECT
    count(),
    {QUANTILES_SECTION},
FROM default.checks
WHERE
    check_start_time >= now() - INTERVAL {DAYS} DAY
    AND check_name = '{JOB_NAME}'
    AND check_status = 'success'
    AND base_ref = '{BASE_REF}'
"""

JOB_NAMES_QUERY = """
SELECT DISTINCT check_name AS JOB_NAME
FROM default.checks
WHERE
    check_start_time >= NOW() - INTERVAL {DAYS} DAY
    AND check_status = 'success'
    AND base_ref = '{BASE_REF}'
    AND head_ref not LIKE '2%'
    AND head_ref not LIKE 'release/2%'
"""


# Format-friendly helper
def format_quantiles(q_list, column="check_duration_ms"):
    return ",\n    ".join(f"quantile({q / 100:.2f})({column})" for q in q_list)


def format_quantiles_names(q_list):
    return [f"{q}" for q in q_list]


RESULTS = ["runs"] + format_quantiles_names(QUANTILES)


def get_job_stat_for_interval(name, interval_days, overall_statistics):
    job_stats = {"quantiles": {}}

    query = JOB_STATISTICS_QUERY.format(
        JOB_NAME=name,
        QUANTILES_SECTION=format_quantiles(QUANTILES),
        DAYS=interval_days,
        BASE_REF=BASE_REF,
    )

    output = cidb.query(query)
    values = output.split()

    key, val = None, None
    try:
        assert len(values) == len(RESULTS), f"Mismatch: {len(values)} vs {len(RESULTS)}"
        for key, val in zip(RESULTS, values):
            parsed_val = int(float(val))
            if key == "runs":
                job_stats["runs"] = parsed_val
            else:
                assert int(key) >= 0, f"Invalid quantile key: {key}"
                job_stats["quantiles"][key] = parsed_val

        overall_statistics[name][f"{interval_days}d"] = job_stats
    except Exception as e:
        print(
            f"  ERROR: Failed to parse stats — key [{key}], value [{val}], error: {e}"
        )
        return False
    return True


if __name__ == "__main__":

    cidb = CIDB(
        url=Secret.Config(
            name="clickhouse-test-stat-url",
            type=Secret.Type.AWS_SSM_VAR,
        ).get_value(),
        user=Secret.Config(
            name="clickhouse-test-stat-login",
            type=Secret.Type.AWS_SSM_VAR,
        ).get_value(),
        passwd=Secret.Config(
            name="clickhouse-test-stat-password",
            type=Secret.Type.AWS_SSM_VAR,
        ).get_value(),
    )

    BASE_REF = "master"

    names = None
    results = []

    print(f"--- Get Job Names ---")

    def get_all_job_names():
        query = JOB_NAMES_QUERY.format(DAYS=3, BASE_REF=BASE_REF)
        global names
        names = cidb.query(query).splitlines()
        return True

    results.append(
        Result.from_commands_run(
            name="Get all job names", command=get_all_job_names, with_info=True
        )
    )
    if not results[-1].is_ok():
        sys.exit()

    print(f"--- Get statistics for each job ---")
    overall_statistics = {}
    is_collected = False
    results_stat = []
    for job_name in names:
        overall_statistics[job_name] = {}

        def do():
            res = False
            for days in DAYS:
                res = (
                    get_job_stat_for_interval(
                        name=job_name,
                        interval_days=days,
                        overall_statistics=overall_statistics,
                    )
                    or res
                )
            return res

        results_stat.append(Result.from_commands_run(name=job_name, command=do))

        if results_stat[-1].is_ok():
            is_collected = True
    results.append(
        Result(
            name="Fetch statistics",
            status=Result.Status.SUCCESS if is_collected else Result.Status.FAILED,
            results=results_stat,
        )
    )

    print(f"--- Upload statistics ---")
    statistics_link = None
    if is_collected:

        def do():
            global statistics_link
            file_name = "./ci/tmp/statistics.json"
            archive_name = "./ci/tmp/statistics.json.gz"
            archive_name_with_date = (
                f"./ci/tmp/statistics_{datetime.now().strftime('%d_%m_%Y')}.json.gz"
            )

            with open(file_name, "w") as f:
                json.dump(overall_statistics, f, indent=2)

            Shell.check(
                f"rm -f {archive_name} {archive_name_with_date} && gzip -k {file_name} && cp {archive_name} {archive_name_with_date}"
            )
            _ = S3.copy_file_to_s3(
                local_path=archive_name,
                s3_path=f"{S3_REPORT_BUCKET_NAME}/statistics",
                content_type="application/json",
                content_encoding="gzip",
            )
            statistics_link = S3.copy_file_to_s3(
                local_path=archive_name_with_date,
                s3_path=f"{S3_REPORT_BUCKET_NAME}/statistics",
                content_type="application/json",
                content_encoding="gzip",
            )

        results.append(Result.from_commands_run("Upload", command=do))

    Result.create_from(
        results=results, links=[statistics_link] if statistics_link else []
    ).complete_job(with_job_summary_in_info=False)
