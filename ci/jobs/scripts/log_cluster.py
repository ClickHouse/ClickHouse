import time
import traceback

import requests

from ci.praktika.info import Info
from ci.praktika.secret import Secret


class LogCluster:
    URL_SECRET = "clickhouse_ci_logs_host"
    PASSWD_SECRET = "clickhouse_ci_logs_password"
    USER = "ci"

    def __init__(self):
        self.user = "ci"
        self.url = ""
        self._session = None
        self._auth = None

    def close_session(self):
        if self._session:
            self._session.close()
            self._session = None

    def is_ready(self):
        if not self.url:
            url = Secret.Config(
                name=self.URL_SECRET,
                type=Secret.Type.AWS_SSM_PARAMETER,
            ).get_value()
            self.url = "https://" + url.removeprefix("https://")
        passwd = Secret.Config(
            name=self.PASSWD_SECRET,
            type=Secret.Type.AWS_SSM_PARAMETER,
        ).get_value()
        if not self.url:
            print("ERROR: failed to retrive password for LogCluster")
            return False
        if not passwd:
            print("ERROR: failed to retrive password for LogCluster")
            return False
        self._auth = {
            "X-ClickHouse-User": self.USER,
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

        response = None
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
                        f"WARNING: LogCluster query failed with code {response.status_code}"
                    )
                if response.status_code >= 500:
                    # A retryable error
                    time.sleep(1)
                    continue
                else:
                    break
            except Exception:
                print(f"WARNING: LogCluster query failed with exception")
                traceback.print_exc()
        if response is not None:
            print(
                f"ERROR: Failed to query LogCluster, query:\n {query}\n    reason:\n {response.text}"
            )
        return False


class LogClusterBuildProfileQueries:

    def __init__(self):
        self._info = Info()
        self._log_cluster = LogCluster()

    def insert_profile_data(self, build_name, start_time, file):
        query = self._profile_query(build_name, start_time)
        with open(file, "rb") as data_fd:
            assert self._log_cluster.do_query(query, data=data_fd, timeout=50)

    def insert_build_size_data(self, build_name, start_time, file):
        query = self._build_size_query(build_name, start_time)
        with open(file, "rb") as data_fd:
            assert self._log_cluster.do_query(query, data=data_fd, timeout=50)

    def insert_binary_symbol_data(self, build_name, start_time, file):
        query = self._binary_symbol_query(build_name, start_time)
        with open(file, "rb") as data_fd:
            assert self._log_cluster.do_query(query, data=data_fd, timeout=50)

    def _profile_query(self, build_name, start_time):
        return f"""INSERT INTO build_time_trace
    (
        pull_request_number,
        commit_sha,
        check_start_time,
        check_name,
        instance_type,
        instance_id,
        file,
        library,
        time,
        pid,
        tid,
        ph,
        ts,
        dur,
        cat,
        name,
        detail,
        count,
        avgMs,
        args_name
    )
    SELECT {self._info.pr_number}, '{self._info.sha}', '{start_time}', '{build_name}', '{self._info.instance_type}', '{self._info.instance_id}', *
    FROM input('
        file String,
        library String,
        time DateTime64(6),
        pid UInt32,
        tid UInt32,
        ph String,
        ts UInt64,
        dur UInt64,
        cat String,
        name String,
        detail String,
        count UInt64,
        avgMs UInt64,
        args_name String')
    FORMAT JSONCompactEachRow"""

    def _build_size_query(self, build_name, start_time):
        return f"""INSERT INTO binary_sizes
    (
        pull_request_number,
        commit_sha,
        check_start_time,
        check_name,
        instance_type,
        instance_id,
        file,
        size
    )
    SELECT {self._info.pr_number}, '{self._info.sha}', '{start_time}', '{build_name}', '{self._info.instance_type}', '{self._info.instance_id}', file, size
    FROM input('size UInt64, file String')
    SETTINGS format_regexp = '^\\s*(\\d+) (.+)$'
    FORMAT Regexp"""

    def _binary_symbol_query(self, build_name, start_time):
        return f"""INSERT INTO binary_symbols
    (
        pull_request_number,
        commit_sha,
        check_start_time,
        check_name,
        instance_type,
        instance_id,
        file,
        address,
        size,
        type,
        symbol
    )
    SELECT {self._info.pr_number}, '{self._info.sha}', '{start_time}', '{build_name}', '{self._info.instance_type}', '{self._info.instance_id}',
    file, reinterpretAsUInt64(reverse(unhex(address))), reinterpretAsUInt64(reverse(unhex(size))), type, symbol
    FROM input('file String, address String, size String, type String, symbol String')
    SETTINGS format_regexp = '^([^ ]+) ([0-9a-fA-F]+)(?: ([0-9a-fA-F]+))? (.) (.+)$'
    FORMAT Regexp"""


if __name__ == "__main__":
    LogCluster = LogCluster()
    assert LogCluster.is_ready()
