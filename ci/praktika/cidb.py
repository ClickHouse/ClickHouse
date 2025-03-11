import copy
import dataclasses
import json
import urllib
from typing import Optional

import requests
from praktika.info import Info

from ._environment import _Environment
from .result import Result
from .settings import Settings
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

    @classmethod
    def _get_sub_result_with_test_cases(cls, result: Result) -> Result:
        if len(result.results) > 20:
            return result
        for sub_result in result.results:
            if sub_result.name.lower() in [
                n.lower() for n in Settings.SUB_RESULT_NAMES_WITH_TESTS
            ]:
                return sub_result
        return result

    @classmethod
    def json_data_generator(cls, result: Result):
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
            report_url=Info().get_report_url(),
            pull_request_url=env.CHANGE_URL,
            base_ref=env.BASE_BRANCH,
            base_repo=env.REPOSITORY,
            head_ref=env.BRANCH,
            head_repo=env.REPOSITORY,  # TODO: remove from table?
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

        test_cases_result = cls._get_sub_result_with_test_cases(result)
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

    def insert(self, result: Result):
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": f"INSERT INTO {Settings.CI_DB_TABLE_NAME} FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }
        assert Settings.CI_DB_TABLE_NAME and Settings.CI_DB_TABLE_NAME

        MAX_RETRIES = 3
        jsons = []
        for json_str in self.json_data_generator(result):
            jsons.append(json_str)

        for retry in range(MAX_RETRIES):
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
                    if retry == MAX_RETRIES - 1:
                        raise RuntimeError(
                            f"Failed to write to CI DB, response code [{response.status_code}]"
                        )
            except Exception as ex:
                if retry == MAX_RETRIES - 1:
                    raise ex

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
