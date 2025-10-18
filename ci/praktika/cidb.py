import copy
import dataclasses
import json
import urllib
from typing import Optional

from ._environment import _Environment
from .info import Info

try:
    import requests
except ImportError as ex:
    if not Info().is_local_run:
        raise ex
    else:
        print(
            f"WARNING: 'requests' module is not installed: {ex}. CIDB will not work - ok for local runs only."
        )

from .result import Result
from .settings import Settings
from .usage import ComputeUsage, StorageUsage
from .utils import Utils


class CIDB:
    @dataclasses.dataclass
    class TableRecord:
        pull_request_number: int
        commit_sha: str
        commit_url: str
        check_name: str
        check_status: str
        check_duration_ms: int
        check_start_time: int
        report_url: str
        pull_request_url: str
        base_ref: str
        base_repo: str
        head_ref: str
        head_repo: str
        task_url: str
        instance_type: str
        instance_id: str
        test_name: str
        test_status: str
        test_duration_ms: Optional[int]
        test_context_raw: str

    def __init__(self, url, user, passwd):
        self.url = url
        self.auth = {
            "X-ClickHouse-User": user,
            "X-ClickHouse-Key": passwd,
        }

    def get_link_to_test_case_statistics(
        self, test_name: str, job_name: Optional[str] = None, url="", user=""
    ) -> str:
        """
        Build a link to query CI DB statistics for a specific test case.
        The link format follows the Play-style URL: <self.url>[?user=<user>]#<base64(sql)>.
        Groups by day and shows recent failures for the test. Optionally filters by job_name.
        """
        # Basic sanitization for SQL string literals
        tn = (test_name or "").replace("'", "''")
        jn = (job_name or "").replace("'", "''") if job_name else None
        # Prefer configured table name if available, fall back to default
        table = Settings.CI_DB_TABLE_NAME or "checks"

        query = f"""\
WITH
    90 AS interval_days
SELECT
    toStartOfDay(check_start_time) AS day,
    count() AS failures,
    groupUniqArray(pull_request_number) AS prs,
    any(report_url) AS report_url
FROM {table}
WHERE (now() - toIntervalDay(interval_days)) <= check_start_time
    AND test_name = '{tn}'
    -- AND check_name = '{job_name}'
    AND test_status IN ('FAIL', 'ERROR')
    AND ((head_ref = 'master' AND pull_request_number = 0) OR pull_request_number != 0)
GROUP BY day
ORDER BY day DESC
"""

        # Compose base URL, optionally attaching user parameter
        base = url or self.url or ""
        if user:
            sep = "&" if "?" in base else "?"
            base = f"{base}/play{sep}user={urllib.parse.quote(user, safe='')}&run=1"
        return f"{base}#{Utils.to_base64(query)}"

    @classmethod
    def _get_sub_result_with_test_cases(
        cls, result: Result, result_name_for_cidb
    ) -> Optional[Result]:
        if not result_name_for_cidb:
            return result
        for r in result.results:
            if r.name == result_name_for_cidb:
                return r
        return None

    @classmethod
    def json_data_generator(cls, result: Result, result_name_for_cidb):
        """Generates JSON data records for the result and its test cases."""
        env = _Environment.get()

        # Create the base record
        base_record = cls.TableRecord(
            pull_request_number=env.PR_NUMBER,
            commit_sha=env.SHA,
            commit_url=env.COMMIT_URL,
            check_name=result.name,
            check_status=result.status,
            check_duration_ms=int(result.duration * 1000) if result.duration else None,
            check_start_time=Utils.timestamp_to_str(result.start_time),
            report_url=Info().get_job_report_url(),
            pull_request_url=env.CHANGE_URL,
            base_ref=env.BASE_BRANCH,
            base_repo=env.REPOSITORY,
            head_ref=env.BRANCH,
            head_repo=env.FORK_NAME,
            task_url="",
            instance_type=",".join(
                filter(None, [env.INSTANCE_TYPE, env.INSTANCE_LIFE_CYCLE])
            ),
            instance_id=env.INSTANCE_ID,
            test_name="",
            test_status="",
            test_duration_ms=None,
            test_context_raw=result.info,
        )
        yield json.dumps(dataclasses.asdict(base_record))

        test_cases_result = cls._get_sub_result_with_test_cases(
            result, result_name_for_cidb
        )
        if test_cases_result:
            for result_ in test_cases_result.results:
                record = copy.copy(base_record)
                record.test_name = result_.name
                record.report_url = (
                    record.report_url
                    + f"&name_1={urllib.parse.quote(result.name, safe='')}"
                )
                if result_.start_time:
                    record.check_start_time = Utils.timestamp_to_str(result_.start_time)
                record.test_status = result_.status
                if result_.duration:
                    record.test_duration_ms = int(result_.duration * 1000)
                record.test_context_raw = result_.info
                yield json.dumps(dataclasses.asdict(record))

    def query(self, query: str, retries: int = 1):
        """
        Executes a SELECT query on CI DB with retry support.

        :param query: SQL query string
        :param retries: Number of retry attempts on failure
        :return: Response text if successful
        """
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": query,
            "send_logs_level": "warning",
        }

        for attempt in range(1, retries + 1):
            try:
                response = requests.post(
                    url=self.url,
                    params=params,
                    headers=self.auth,
                    timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                )

                if response.ok:
                    return response.text
                else:
                    print(
                        f"WARNING: CIDB query failed (status {response.status_code}) - Attempt {attempt}"
                    )
                    if attempt == retries:
                        raise RuntimeError(
                            f"Failed to query CI DB. Response code: {response.status_code}, Body: {response.text}"
                        )

            except Exception as ex:
                print(f"ERROR: Exception during CI DB query attempt {attempt}: {ex}")
                if attempt == retries:
                    raise ex

    def insert_rows(self, jsons, retries=3):
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": f"INSERT INTO {Settings.CI_DB_TABLE_NAME} FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }
        assert Settings.CI_DB_TABLE_NAME

        for retry in range(retries):
            try:
                response = requests.post(
                    url=self.url,
                    params=params,
                    data=",".join(jsons),
                    headers=self.auth,
                    timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                )
                print(response.text)
                if response.ok:
                    print(f"INFO: {len(jsons)} rows inserted into CIDB")
                    break
                else:
                    if retry == retries - 1:
                        raise RuntimeError(
                            f"Failed to write to CI DB, response code [{response.status_code}]"
                        )
            except Exception as ex:
                if retry == retries - 1:
                    raise ex

    def insert(self, result: Result, result_name_for_cidb=""):
        jsons = []
        for json_str in self.json_data_generator(result, result_name_for_cidb):
            jsons.append(json_str)
        self.insert_rows(jsons)
        return self

    def insert_storage_usage(self, storage_usage: StorageUsage):
        info = Info()
        json_rows = []
        record = self.TableRecord(
            pull_request_number=info.pr_number,
            commit_sha=info.sha,
            commit_url=info.commit_url,
            check_name="Usage Storage",
            check_status=Result.Status.SUCCESS,
            check_duration_ms=storage_usage.uploaded,
            check_start_time=Utils.timestamp_to_str(Utils.timestamp()),
            report_url=info.get_report_url(),
            pull_request_url=info.change_url,
            base_ref=info.base_branch,
            base_repo=info.repo_name,
            head_ref=info.git_branch,
            head_repo=info.fork_name,
            task_url="",
            instance_type=",".join(
                filter(None, [info.instance_type, info.instance_lifecycle])
            ),
            instance_id=info.instance_id,
            test_name="storage_usage_uploaded_bytes",
            test_status="OK",
            test_duration_ms=0,
            test_context_raw="test_duration_ms shows total size uploaded in bytes",
        )
        json_rows.append(json.dumps(dataclasses.asdict(record)))
        record.test_name = "storage_usage_uploaded_items"
        record.test_duration_ms = len(storage_usage.uploaded_details)
        record.test_context_raw = (
            "test_duration_ms shows total number of uniq object names uploaded"
        )
        json_rows.append(json.dumps(dataclasses.asdict(record)))
        self.insert_rows(json_rows)
        return self

    def insert_compute_usage(self, compute_usage: ComputeUsage):
        info = Info()
        json_rows = []
        for runner_str, usage in compute_usage.runners_usage.items():
            jobs = sorted(compute_usage.details[runner_str])
            description = ",".join(jobs)
            record = self.TableRecord(
                pull_request_number=info.pr_number,
                commit_sha=info.sha,
                commit_url=info.commit_url,
                check_name="Usage Compute",
                check_status=Result.Status.SUCCESS,
                check_duration_ms=int(usage * 1000),
                check_start_time=Utils.timestamp_to_str(Utils.timestamp()),
                report_url=info.get_report_url(),
                pull_request_url=info.change_url,
                base_ref=info.base_branch,
                base_repo=info.repo_name,
                head_ref=info.git_branch,
                head_repo=info.fork_name,
                task_url="",
                instance_type=",".join(
                    filter(None, [info.instance_type, info.instance_lifecycle])
                ),
                instance_id=info.instance_id,
                test_name=runner_str,
                test_status="OK",
                test_duration_ms=0,
                test_context_raw=description,
            )
            json_rows.append(json.dumps(dataclasses.asdict(record)))
        self.insert_rows(json_rows)
        return self

    def check(self):
        # Create a session object
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": f"SELECT 1",
        }
        error = ""
        for retry in range(2):
            try:
                response = requests.post(
                    url=self.url,
                    params=params,
                    data="",
                    headers=self.auth,
                    timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                )
                if not response.ok:
                    error = f"ERROR: No connection to CI DB [{response.status_code}/{response.reason}]"
                elif not response.json() == 1:
                    print("ERROR: CI DB smoke test failed select 1 == 1")
                    error = f"ERROR: CI DB smoke test failed [select 1 ==> {response.json()}]"
                else:
                    return True, ""

            except Exception as ex:
                print(f"ERROR: Exception [{ex}]")
                error = f"CIDB: ERROR: Exception [{ex}]"

        return False, error
