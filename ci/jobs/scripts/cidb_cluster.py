import json
import os
import re
import time
import traceback

import requests

from ci.praktika.info import Info
from ci.praktika.settings import Settings


class CIDBCluster:
    URL_SECRET = Settings.SECRET_CI_DB_URL
    PASSWD_SECRET = Settings.SECRET_CI_DB_PASSWORD
    USER_SECRET = Settings.SECRET_CI_DB_USER

    def __init__(self, url=None, user=None, pwd=None):
        info = Info()
        if url and user is not None and pwd is not None:
            self.url_secret = None
            self.user_secret = None
            self.pwd_secret = None
            self.url = url
            self.user = user
            self.pwd = pwd
        else:
            self.user_secret = info.get_secret(self.USER_SECRET)
            self.url_secret = info.get_secret(self.URL_SECRET)
            self.pwd_secret = info.get_secret(self.PASSWD_SECRET)
            self.user = None
            self.url = None
            self.pwd = None
        self._session = None
        self._auth = {}

    def close_session(self):
        if self._session:
            self._session.close()
            self._session = None

    def is_ready(self):
        if not self.url:
            self.url, self.user, self.pwd = (
                self.url_secret.join_with(self.user_secret)
                .join_with(self.pwd_secret)
                .get_value()
            )
            if not self.url:
                print("ERROR: failed to retrieve password for LogCluster")
                return False
            if not self.pwd:
                print("ERROR: failed to retrieve password for LogCluster")
                return False
        if self.pwd and not self._auth:
            self._auth = {
                "X-ClickHouse-User": self.user,
                "X-ClickHouse-Key": self.pwd,
            }
        params = {
            "query": f"SELECT 1",
        }
        try:
            response = requests.post(
                url=self.url,
                params=params,
                data="",
                headers=self._auth,
                timeout=3,
            )
            if not response.ok:
                print(
                    f"ERROR: No connection to cluster [{self.url}]: [{response.text}]"
                )
                return False
            if not response.json() == 1:
                print("ERROR: LogCluster failure 1 != 1")
                return False
        except Exception as ex:
            print(f"ERROR: LogCluster connection failed with exception [{ex}]")
            return False
        return True

    def do_select_query(self, query, db_name="", retries=1, timeout=5):
        if not self.is_ready():
            print("ERROR: LogCluster not ready")
            return None

        if not self._session:
            self._session = requests.Session()

        params = {
            "query": query,
        }
        if db_name:
            params["database"] = db_name

        for retry in range(retries):
            try:
                response = self._session.get(
                    url=self.url,
                    params=params,
                    headers=self._auth,
                    timeout=timeout,
                )
                if response.ok:
                    return response.text
                else:
                    print(f"WARNING: CIDB query failed: {response.text}")
                    if response.status_code >= 500:
                        time.sleep(2**retry)  # exponential backoff
                        continue
                    else:
                        break
            except requests.RequestException as ex:
                print(f"WARNING: CIDB query failed with exception: {ex}")
                traceback.print_exc()

        print("ERROR: Failed to do select query CIDB")
        return None

    def do_insert_query(self, query, data, db_name="", retries=1, timeout=5):
        if not self.is_ready():
            print("ERROR: LogCluster not ready")
            return False

        if not self._session:
            self._session = requests.Session()

        params = {
            "query": query,
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }
        if db_name:
            params["database"] = db_name

        for retry in range(retries):
            try:
                response = self._session.post(
                    url=self.url,
                    params=params,
                    data=data,
                    headers=self._auth,
                    timeout=timeout,
                )
                if response.ok:
                    return True
                else:
                    print(
                        f"WARNING: CIDB query failed with code {response.status_code}, text {response.text}"
                    )
                if response.status_code >= 500:
                    # A retryable error
                    time.sleep(1)
                    continue
                else:
                    break
            # TODO
            # except as ex:
            #     print(f"WARNING: CIDB query failed with exception: {ex} - retry")
            except Exception as ex:
                print(f"ERROR: CIDB query failed with exception: {ex}")
                traceback.print_exc()
                break
        print(f"ERROR: Failed to query CIDB")
        return False

    def insert_json(self, table, json_str):
        if isinstance(json_str, dict):
            json_str = json.dumps(json_str)
        self.do_insert_query(
            query=f"INSERT INTO {table} FORMAT JSONEachRow", data=json_str
        )
        self.close_session()

    def insert_keeper_metrics_from_file(
        self,
        file_path: str,
        chunk_size: int = 1000,
        retries: int = 3,
    ):
        if not self.is_ready():
            print("ERROR: CIDBCluster not ready, skipping keeper metrics ingestion")
            return 0, 0
        metrics_db = Settings.KEEPER_STRESS_METRICS_DB_NAME
        table = Settings.KEEPER_STRESS_METRICS_TABLE_NAME
        for _ident in (metrics_db, table):
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", _ident):
                raise ValueError(f"Invalid identifier for keeper metrics table: {_ident!r}")
        if not file_path or not os.path.exists(file_path):
            return 0, 0

        insert_params = {
            "database": metrics_db,
            "query": f"INSERT INTO {metrics_db}.{table} FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }

        def _insert_chunk(lines: list) -> int:
            if not lines:
                return 0
            body = "\n".join(lines)
            last_status = None
            for attempt in range(retries):
                response = requests.post(
                    url=self.url,
                    params=insert_params,
                    data=body,
                    headers=self._auth,
                    timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                )
                last_status = response.status_code
                if response.ok:
                    return len(lines)
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
            raise RuntimeError(
                f"Failed to write keeper metrics after {retries} attempts, last response code [{last_status}]"
            )

        inserted = 0
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            chunk = []
            for line in f:
                s = line.strip()
                if not s:
                    continue
                chunk.append(s)
                if len(chunk) >= chunk_size:
                    inserted += _insert_chunk(chunk)
                    chunk = []
            inserted += _insert_chunk(chunk)

        print(f"INFO: keeper metrics inserted: {inserted}")
        return inserted, 0


if __name__ == "__main__":
    CIDBCluster = CIDBCluster(
        url="https://play.clickhouse.com?user=play", user="", pwd=""
    )
    assert CIDBCluster.is_ready()
