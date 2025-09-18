import json
import time
import traceback

import requests
from praktika.info import Info
from praktika.settings import Settings


class CIDBCluster:
    URL_SECRET = Settings.SECRET_CI_DB_URL
    PASSWD_SECRET = Settings.SECRET_CI_DB_PASSWORD
    USER_SECRET = Settings.SECRET_CI_DB_USER

    def __init__(self):
        info = Info()
        self.user_secret = info.get_secret(self.USER_SECRET)
        self.url_secret = info.get_secret(self.URL_SECRET)
        self.pwd_secret = info.get_secret(self.PASSWD_SECRET)
        self.user = None
        self.url = None
        self.pwd = None
        self._session = None
        self._auth = None

    def close_session(self):
        if self._session:
            self._session.close()
            self._session = None

    def is_ready(self):
        if not self.url:
            self.url = self.url_secret.get_value()
            self.user = self.user_secret.get_value()
            passwd = self.pwd_secret.get_value()
            if not self.url:
                print("ERROR: failed to retrive password for LogCluster")
                return False
            if not passwd:
                print("ERROR: failed to retrive password for LogCluster")
                return False
            self._auth = {
                "X-ClickHouse-User": self.user,
                "X-ClickHouse-Key": passwd,
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
                print("ERROR: No connection to LogCluster")
                return False
            if not response.json() == 1:
                print("ERROR: LogCluster failure 1 != 1")
                return False
        except Exception as ex:
            print(f"ERROR: LogCluster connection failed with exception [{ex}]")
            return False
        return True

    def do_query(self, query, data, db_name="", retries=1, timeout=5):
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
                        f"WARNING: CIDB query failed with code {response.status_code}"
                    )
                if response.status_code >= 500:
                    # A retryable error
                    time.sleep(1)
                    continue
                else:
                    break
            except Exception as ex:
                print(f"WARNING: CIDB query failed with exception")
                traceback.print_exc()
        print(f"ERROR: Failed to query CIDB")
        return False

    def insert_json(self, table, json_str):
        if isinstance(json_str, dict):
            json_str = json.dumps(json_str)
        self.do_query(query=f"INSERT INTO {table} FORMAT JSONEachRow", data=json_str)
        self.close_session()


if __name__ == "__main__":
    CIDBCluster = CIDBCluster()
    assert CIDBCluster.is_ready()
