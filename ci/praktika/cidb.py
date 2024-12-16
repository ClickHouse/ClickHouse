import copy
import dataclasses
import json
from typing import Optional

import requests
from praktika._environment import _Environment
from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import Utils


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

    def __init__(self, url, passwd):
        self.url = url
        self.auth = {
            "X-ClickHouse-User": "default",
            "X-ClickHouse-Key": passwd,
        }

    @classmethod
    def json_data_generator(cls, result: Result):
        env = _Environment.get()
        base_record = cls.TableRecord(
            pull_request_number=env.PR_NUMBER,
            commit_sha=env.SHA,
            commit_url=env.COMMIT_URL,
            check_name=result.name,
            check_status=result.status,
            check_duration_ms=int(result.duration * 1000),
            check_start_time=Utils.timestamp_to_str(result.start_time),
            report_url=env.get_report_url(),
            pull_request_url=env.CHANGE_URL,
            base_ref=env.BASE_BRANCH,
            base_repo=env.REPOSITORY,
            head_ref=env.BRANCH,
            # TODO: remove from table?
            head_repo=env.REPOSITORY,
            # TODO: remove from table?
            task_url="",
            instance_type=",".join([env.INSTANCE_TYPE, env.INSTANCE_LIFE_CYCLE]),
            instance_id=env.INSTANCE_ID,
            test_name="",
            test_status="",
            test_duration_ms=None,
            test_context_raw=result.info,
        )
        yield json.dumps(dataclasses.asdict(base_record))
        for result_ in result.results:
            record = copy.deepcopy(base_record)
            record.test_name = result_.name
            if result_.start_time:
                record.check_start_time = (Utils.timestamp_to_str(result.start_time),)
            record.test_status = result_.status
            record.test_duration_ms = int(result_.duration * 1000)
            record.test_context_raw = result_.info
            yield json.dumps(dataclasses.asdict(record))

    def insert(self, result: Result):
        # Create a session object
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": f"INSERT INTO {Settings.CI_DB_TABLE_NAME} FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }

        session = requests.Session()

        for json_str in self.json_data_generator(result):
            try:
                response1 = session.post(
                    url=self.url,
                    params=params,
                    data=json_str,
                    headers=self.auth,
                    timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                )
            except Exception as ex:
                raise ex

        session.close()

    def check(self):
        # Create a session object
        params = {
            "database": Settings.CI_DB_DB_NAME,
            "query": f"SELECT 1",
        }
        try:
            response = requests.post(
                url=self.url,
                params=params,
                data="",
                headers=self.auth,
                timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
            )
            if not response.ok:
                print("ERROR: No connection to CI DB")
                return (
                    False,
                    f"ERROR: No connection to CI DB [{response.status_code}/{response.reason}]",
                )
            if not response.json() == 1:
                print("ERROR: CI DB smoke test failed select 1 == 1")
                return (
                    False,
                    f"ERROR: CI DB smoke test failed [select 1 ==> {response.json()}]",
                )
        except Exception as ex:
            print(f"ERROR: Exception [{ex}]")
            return False, "CIDB: ERROR: Exception [{ex}]"
        return True, ""
