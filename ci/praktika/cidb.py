import copy
import dataclasses
import json
import time
import urllib
from typing import List, Optional

from ._environment import _Environment
from .info import Info

try:
    import requests
except ImportError as ex:
    if not Info().is_local_run:
        raise ex
    else:
        print(
            "WARNING: 'requests' module is not installed: "
            f"{ex}. CIDB will not work - ok for local runs only."
        )

from .result import Result
from .settings import Settings
from .usage import ComputeUsage, PipelineUtilization, StorageUsage
from .utils import Utils


class CIDB:
    _STATUS_TO_CIDB = {
        Result.Status.OK: "success",
        Result.Status.FAIL: "failure",
        Result.Status.ERROR: "error",
        Result.Status.SKIPPED: "skipped",
        Result.Status.PENDING: "pending",
        Result.Status.RUNNING: "running",
        Result.Status.DROPPED: "dropped",
        Result.Status.UNKNOWN: "failure",
        Result.Status.XFAIL: "success",
        Result.Status.XPASS: "failure",
    }

    @classmethod
    def convert_status(cls, status: str) -> str:
        """Map Result.Status value to legacy CIDB check_status string."""
        # An empty status is allowed for synthetic rows (e.g. the workflow-level
        # metrics summary row, which is not a real check).
        if not status:
            return ""
        legacy = cls._STATUS_TO_CIDB.get(status)
        if legacy is not None:
            return legacy
        # Already a legacy string — pass through for idempotency
        assert (
            status in cls._STATUS_TO_CIDB.values()
        ), f"Invalid status [{status}] for CIDB check_status"
        return status

    @dataclasses.dataclass
    class TableRecord:
        pull_request_number: int
        commit_sha: str
        workflow_name: str
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
        # Structured metrics for the `attributes` JSON column: per-job host
        # utilization on job rows, workflow-level usage on the summary row.
        attributes: dict = dataclasses.field(default_factory=dict)

        def __post_init__(self):
            # Transparently convert Result.Status values to legacy CIDB strings
            self.check_status = CIDB.convert_status(self.check_status)

    def __init__(self, url, user=None, passwd=None):
        # Falsy user/passwd (None, empty string, JSON null) means "send no
        # auth header" — used when the runner-facing CH user is configured
        # with <no_password/> + a network ACL on the server side.
        self.url = url
        self.auth = {}
        if user:
            self.auth["X-ClickHouse-User"] = user
        if passwd:
            self.auth["X-ClickHouse-Key"] = passwd

    @classmethod
    def from_connection_secret(cls, connection_str: str) -> "CIDB":
        """Build a CIDB from a JSON connection blob in SSM Parameter Store.

        The blob must have a ``url`` field and may have ``user``/``password``
        fields. Null/empty/missing user or password mean "send no auth" so
        the runner side stays creds-free when the server enforces auth via
        network ACLs (`<no_password/>` user scoped to the VPC CIDR).

        Example::

            {"url": "http://10.0.42.144:8123", "user": null, "password": null}
            {"url": "http://...", "user": "admin", "password": "..."}
        """
        data = json.loads(connection_str)
        return cls(
            url=data["url"],
            user=data.get("user"),
            passwd=data.get("password"),
        )

    def get_link_to_test_case_statistics(
        self,
        test_name: str,
        job_name: Optional[str] = None,
        failure_patterns=None,
        test_output="",
        url="",
        user="",
        pr_base_branches: Optional[List[str]] = None,
    ) -> str:
        """
        Build a link to query CI DB statistics for a specific test case.

        The generated link includes a SQL query that filters historical failures by the same pattern
        found in the current test output. This helps narrow down similar failures and check their history.

        Args:
            test_name: Name of the test case
            job_name: Optional job name for filtering
            failure_patterns: List of substring patterns configured in CI settings (Settings.TEST_FAILURE_PATTERNS)
            test_output: The current test failure output to match against patterns
            url: Optional base URL (defaults to self.url)
            user: Optional username for ClickHouse Play authentication
            pr_base_branches: Optional list of base branches to include PR runs. If provided, includes PRs targeting any of these branches. If omitted, only main branch runs are included.

        Pattern Matching Logic:
            - Scans test_output for first matching pattern from failure_patterns list
            - If match found: adds "AND test_context_raw LIKE '%pattern%'" filter to SQL query
            - If no match: adds commented placeholder for manual editing
            - This ensures the CIDB link in PR comments and CI reports shows only relevant historical failures

        Returns:
            URL with base64-encoded SQL query for viewing test failure history
        """
        # Basic sanitization for SQL string literals
        tn = (test_name or "").replace("'", "''")
        jn = (job_name or "").replace("'", "''") if job_name else None
        # Prefer configured table name if available, fall back to default
        table = Settings.CI_DB_TABLE_NAME or "checks"

        # Find first matching failure pattern in test output
        matched_pattern = None
        if failure_patterns and test_output:
            for pattern in failure_patterns:
                if pattern in test_output:
                    # Sanitize pattern for SQL and use first match
                    matched_pattern = pattern.replace("'", "''")
                    break

        # Build failure pattern filter line
        if matched_pattern:
            failure_filter = f"    AND test_context_raw LIKE '%{matched_pattern}%'"
        else:
            # Add commented placeholder for manual editing
            failure_filter = "    -- AND test_context_raw LIKE '%pattern%'  -- uncomment and edit to filter by failure pattern"

        # Build PR filter based on pr_base_branches parameter
        if pr_base_branches:
            pr_base_branches = list(set(pr_base_branches))
            # Sanitize branch names and build IN clause
            sanitized_branches = [
                branch.replace("'", "''") for branch in pr_base_branches
            ]
            branches_list = ", ".join(f"'{branch}'" for branch in sanitized_branches)
            # Include both main branch runs and PRs targeting any of the specified base branches
            pr_filter = (
                f"    AND (pull_request_number = 0 OR base_ref IN ({branches_list}))"
            )
        else:
            # Only include main branch runs
            pr_filter = "    AND pull_request_number = 0"

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
    -- AND check_name = '{jn}'
    AND test_status IN ('FAIL', 'ERROR')
{pr_filter}
{failure_filter}
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

    @staticmethod
    def _host_usage_attributes(metrics) -> dict:
        """Build the per-job ``attributes`` payload from the compacted host
        metrics dict (see ``HostMetricsCollector``): peak CPU/iowait/RAM/disk
        usage and PSI stall totals. Returns a flat dict of scalar leaves (no
        nested objects/arrays) for the ``attributes`` JSON column, or ``{}``
        when metrics are missing or predate the ``peaks`` schema."""
        if not metrics or not metrics.get("peaks"):
            return {}
        peaks = metrics.get("peaks", {})
        averages = metrics.get("averages", {})
        psi = metrics.get("psi", {})
        attrs = {
            "host_cpu_count": metrics.get("cpu_count"),
            "host_duration_s": metrics.get("duration"),
            "host_mem_total_gb": metrics.get("mem_total_gb"),
            "host_cpu_peak_pct": peaks.get("cpu"),
            "host_iowait_peak_pct": peaks.get("iowait"),
            "host_mem_peak_pct": peaks.get("mem"),
            "host_mem_peak_gb": peaks.get("mem_gb"),
            "host_cpu_avg_pct": averages.get("cpu"),
            "host_iowait_avg_pct": averages.get("iowait"),
            "host_mem_avg_pct": averages.get("mem"),
        }
        if "disk_total_gb" in metrics:
            attrs["host_disk_total_gb"] = metrics.get("disk_total_gb")
            attrs["host_disk_peak_pct"] = peaks.get("disk")
            attrs["host_disk_peak_gb"] = peaks.get("disk_gb")
            if "disk" in averages:
                attrs["host_disk_avg_pct"] = averages.get("disk")
        if psi:
            attrs["host_cpu_stall_s"] = psi.get("cpu_s")
            attrs["host_mem_stall_s"] = psi.get("mem_some_s")
            attrs["host_mem_stall_all_s"] = psi.get("mem_full_s")
            attrs["host_io_stall_s"] = psi.get("io_some_s")
            attrs["host_io_stall_all_s"] = psi.get("io_full_s")
        return {k: v for k, v in attrs.items() if v is not None}

    @classmethod
    def json_data_generator(cls, result: Result, result_name_for_cidb):
        """Generates JSON data records for the result and its test cases."""
        env = _Environment.get()

        # Create the base record
        base_record = cls.TableRecord(
            pull_request_number=env.PR_NUMBER,
            commit_sha=env.SHA,
            workflow_name=env.WORKFLOW_NAME,
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
            task_url=Info().get_job_url(),
            instance_type=",".join(
                filter(None, [env.INSTANCE_TYPE, env.INSTANCE_LIFE_CYCLE])
            ),
            instance_id=env.INSTANCE_ID,
            test_name="",
            test_status="",
            test_duration_ms=None,
            test_context_raw=result.info,
            attributes=cls._host_usage_attributes(result.ext.get("metrics")),
        )
        yield json.dumps(dataclasses.asdict(base_record))

        test_cases_result = cls._get_sub_result_with_test_cases(
            result, result_name_for_cidb
        )
        if test_cases_result:
            for result_ in test_cases_result.results:
                record = copy.copy(base_record)
                # Host usage is a job-level metric; keep it only on the job row.
                record.attributes = {}
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

    def _post_with_retries(self, params, data, timeout, retries, what):
        """POST to CI DB, backing off progressively on transport errors and on
        non-OK responses alike: `requests` does not raise for 4xx/5xx."""
        retry = 0
        while True:
            retry += 1
            try:
                response = requests.post(
                    url=self.url,
                    params=params,
                    data=data,
                    headers=self.auth,
                    timeout=timeout,
                )
                if response.ok:
                    return response
                error = f"{what} failed, response code [{response.status_code}], body [{response.text}]"
            except Exception as ex:
                error = f"{what} failed, exception [{ex}]"

            print(f"WARNING: CIDB {error} - attempt {retry}/{retries}")
            if retry >= retries:
                raise RuntimeError(f"CIDB {error}")
            time.sleep(2**retry)

    def query(self, query: str, retries: int = 5, log_level="warning"):
        """
        Executes a SELECT query on CI DB with retry support.

        :param query: SQL query string
        :param retries: Number of retry attempts on failure
        :return: Response text if successful
        """
        params = {
            "database": Settings.CI_DB_DB_NAME,
        }

        if log_level:
            params["send_logs_level"] = log_level

        return self._post_with_retries(
            params=params,
            data=query.encode(),
            timeout=Settings.CI_DB_QUERY_TIMEOUT_SEC,
            retries=retries,
            what="query",
        ).text

    @staticmethod
    def _prepare_request_body(data):
        if isinstance(data, str):
            return data.encode("utf-8")
        return data

    def insert_rows(self, jsons, retries=3, table=""):
        """Insert JSONEachRow records into `table`, by default the main results
        table. Jobs that keep their own table in the CI database pass it here,
        e.g. the `Revert CI regressions` job and `checks_investigated`."""
        table = table or Settings.CI_DB_TABLE_NAME
        assert table
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": f"INSERT INTO {table} FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }

        response = self._post_with_retries(
            params=params,
            data=self._prepare_request_body("\n".join(jsons)),
            timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
            retries=retries,
            what="insert",
        )
        print(response.text)
        print(f"INFO: {len(jsons)} rows inserted into CIDB")

    def insert(self, result: Result, result_name_for_cidb=""):
        jsons = []
        for json_str in self.json_data_generator(result, result_name_for_cidb):
            jsons.append(json_str)
        self.insert_rows(jsons)
        return self

    def insert_workflow_usage(
        self,
        pipeline_utilization: Optional[PipelineUtilization] = None,
        storage_usage: Optional[StorageUsage] = None,
        compute_usage: Optional[ComputeUsage] = None,
        job_counts: Optional[dict] = None,
        start_time: Optional[float] = None,
        duration_s: Optional[float] = None,
        workflow_status: str = "",
    ):
        """Write a single workflow-level summary row carrying pipeline
        utilization, storage and compute usage in the ``attributes`` JSON
        column. Called once from the final (Finish workflow) job.

        ``job_counts`` is a ``{bucket: count}`` breakdown of the pipeline's
        jobs by status (e.g. ``total``/``success``/``failed``/``skipped``),
        written as ``pipeline_<bucket>_jobs``. Note this is distinct from
        ``pipeline_jobs`` below, which counts only the jobs substantial enough
        to qualify for the utilization KPI.

        Replaces the older ``insert_storage_usage``/``insert_compute_usage``,
        which encoded these numbers into the ``check_duration_ms``/``test_*``
        columns in a way inconsistent with their schema meaning."""
        info = Info()
        attributes: dict = {}
        if workflow_status:
            # This is a synthetic metrics row, so the canonical check_status is
            # left empty (it must not be mistaken for the workflow's own status).
            # The real workflow status is carried here instead.
            attributes["pipeline_status"] = self.convert_status(workflow_status)
        if start_time:
            # Actual pipeline start (first job), unlike the row's check_start_time
            # which is stamped when this final job writes the summary.
            attributes["pipeline_start_time"] = Utils.timestamp_to_str(start_time)
        if duration_s is not None:
            # Whole-pipeline wall-clock duration (first job start to last job
            # end), distinct from pipeline_wall_time_s which sums job runtimes.
            attributes["pipeline_duration_s"] = round(duration_s, 1)
        for bucket, count in (job_counts or {}).items():
            attributes[f"pipeline_{bucket}_jobs"] = count
        if pipeline_utilization and pipeline_utilization.jobs:
            for key, value in pipeline_utilization.to_summary().items():
                attributes[f"pipeline_{key}"] = value
        if storage_usage and (storage_usage.uploaded or storage_usage.downloaded):
            attributes["storage_uploaded_bytes"] = storage_usage.uploaded
            attributes["storage_uploaded_items"] = len(storage_usage.uploaded_details)
            attributes["storage_downloaded_bytes"] = storage_usage.downloaded
            attributes["storage_downloaded_items"] = len(
                storage_usage.downloaded_details
            )
        if compute_usage and compute_usage.runners_usage:
            # runner type -> accumulated seconds across all its jobs.
            attributes["compute_usage_seconds"] = {
                runner_str: round(usage, 1)
                for runner_str, usage in compute_usage.runners_usage.items()
            }
        if not attributes:
            print("NOTE: no workflow usage data to insert into CIDB")
            return self
        record = self.TableRecord(
            pull_request_number=info.pr_number,
            commit_sha=info.sha,
            workflow_name=info.workflow_name,
            commit_url=info.commit_url,
            check_name=info.workflow_name,
            check_status="",
            check_duration_ms=0,
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
            test_name="",
            test_status="",
            test_duration_ms=0,
            test_context_raw="",
            attributes=attributes,
        )
        self.insert_rows([json.dumps(dataclasses.asdict(record))])
        return self

    def check(self):
        # Create a session object
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": "SELECT 1",
        }
        try:
            response = self._post_with_retries(
                params=params,
                data="",
                timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                retries=3,
                what="smoke test",
            )
        except Exception as ex:
            return False, f"CIDB: ERROR: no connection to CI DB [{ex}]"

        # A 200 with a non-JSON body (proxy or login page) stays a failed
        # precheck instead of aborting workflow generation
        try:
            payload = response.json()
        except ValueError as ex:
            return (
                False,
                f"ERROR: CI DB smoke test got a non-JSON body [{ex}]: {response.text[:200]}",
            )

        if not payload == 1:
            return False, f"ERROR: CI DB smoke test failed [select 1 ==> {payload}]"
        return True, ""
