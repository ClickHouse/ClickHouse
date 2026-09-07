import time
import traceback

import requests

from ci.praktika.info import Info
from ci.praktika.secret import Secret


class LogCluster:
    URL_SECRET = "clickhouse_ci_logs_host"
    PASSWD_SECRET = "clickhouse_ci_logs_password"
    # A read-only sub-service of the same cluster: it serves the same data with
    # the same user and password, but its own compute. Every read-only query
    # from the CI goes there, so that reporting queries and the log and build
    # profile uploads of the whole CI fleet do not compete for one endpoint.
    # Not a secret, unlike the writer endpoint, hence no AWS SSM parameter.
    READONLY_URL = "https://t6h0zvqlgy.us-east-2.aws.clickhouse-staging.com"
    USER = "ci"

    def __init__(self, url="", user="", password=None, readonly=False):
        # Explicit url/user/password skip the AWS SSM secret lookup - used for
        # running the consumers locally against the cluster.
        self.user = user or self.USER
        self.readonly = readonly
        self.url = url or (self.READONLY_URL if readonly else "")
        self._session = None
        self._auth = None
        if password is not None:
            self._auth = {
                "X-ClickHouse-User": self.user,
                "X-ClickHouse-Key": password,
            }

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
        if not self.url:
            print("ERROR: failed to retrive url for LogCluster")
            return False
        if self._auth is None:
            passwd = Secret.Config(
                name=self.PASSWD_SECRET,
                type=Secret.Type.AWS_SSM_PARAMETER,
            ).get_value()
            if not passwd:
                print("ERROR: failed to retrive password for LogCluster")
                return False
            self._auth = {
                "X-ClickHouse-User": self.user,
                "X-ClickHouse-Key": passwd,
            }
        params = {
            "query": "SELECT 1",
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
        # The INSERT transport: the read-only endpoint cannot serve it, and
        # silently sending data there would lose it.
        assert not self.readonly, "LogCluster: writes need the writer endpoint"
        if not self.is_ready():
            print("ERROR: LogCluster not ready")
            return False

        if not self._session:
            self._session = requests.Session()

        params = {
            "query": query,
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
            # Override the per-user memory limit from the cluster's default
            # profile, which otherwise aborts large INSERTs with
            # "User memory limit exceeded" via the OvercommitTracker.
            "max_memory_usage_for_user": 0,
            # Parse the input on a single thread: parallel parsing buffers many
            # chunks at once, and that peak is what crosses the shared cluster's
            # per-user memory limit when all Build variants upload at once.
            "input_format_parallel_parsing": 0,
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
                print("WARNING: LogCluster query failed with exception")
                traceback.print_exc()
        if response is not None:
            print(
                f"ERROR: Failed to query LogCluster, query:\n {query}\n    reason:\n {response.text}"
            )
        return False

    def select(self, query, retries=8, timeout=60):
        """Run a read-only query and return the response body, or None on failure.

        Unlike do_query (INSERT transport, discards the body), this returns the
        result text. Retries transient (>=500 and connection) errors with a
        growing backoff: the shared cluster goes through minutes-long
        server-wide memory-pressure spikes (Code 241 for every query).
        """
        # The query goes in the body: queries with long IN lists exceed the
        # server's URI length limit as a parameter.
        # Nothing is sent along with it against the read-only endpoint: a
        # read-only profile rejects a query that changes any setting, and none
        # of the settings here is needed to read.
        params = {} if self.readonly else {"send_logs_level": "warning"}

        response = None
        for retry in range(retries):
            # is_ready is a cheap `SELECT 1` and fails during the same pressure
            # spikes as the query itself, so it is retried on the same schedule.
            if not self.is_ready():
                print("WARNING: LogCluster not ready")
                time.sleep(5 * (retry + 1))
                continue
            if not self._session:
                self._session = requests.Session()
            try:
                response = self._session.post(
                    url=self.url,
                    params=params,
                    data=query.encode(),
                    headers=self._auth,
                    timeout=timeout,
                )
                if response.ok:
                    return response.text
                print(
                    f"WARNING: LogCluster select failed with code {response.status_code}"
                )
                if response.status_code >= 500:
                    time.sleep(5 * (retry + 1))
                    continue
                break
            except Exception:
                print("WARNING: LogCluster select failed with exception")
                traceback.print_exc()
                time.sleep(5 * (retry + 1))
        if response is not None:
            print(
                f"ERROR: Failed to select from LogCluster, query:\n {query}\n    reason:\n {response.text}"
            )
        return None


class LogClusterBuildProfileQueries:

    # Event kinds kept in a reduced time-trace upload (PR builds): per-TU
    # compile phases, per-entity frontend events, and the top-level and
    # per-function link/ThinLTO events. This is what the "Build profile diff"
    # check (ci/jobs/build_profile_diff_job.py) consumes. Everything else -
    # notably the millions of per-pass LLVM events of a ThinLTO link - is
    # dropped. Aggregate 'Total *' events are kept unconditionally.
    REDUCED_PROFILE_EVENTS = (
        "ExecuteCompiler",
        "Frontend",
        "Backend",
        "ExecuteLinker",
        "Link",
        "LTO",
        "Thin Link",
        "OptModule",
        "OptFunction",
        "CodeGen Function",
        "InstantiateFunction",
        "InstantiateClass",
        "ParseClass",
        "ParseTemplate",
        "Source",
        "PerformPendingInstantiations",
    )

    def __init__(self):
        self._info = Info()
        self._log_cluster = LogCluster()

    def insert_profile_data(self, build_name, start_time, file, reduced=False):
        query = self._profile_query(build_name, start_time, reduced=reduced)
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

    def _profile_query(self, build_name, start_time, reduced=False):
        where = ""
        if reduced:
            names = ", ".join(f"'{name}'" for name in self.REDUCED_PROFILE_EVENTS)
            where = f"\n    WHERE name IN ({names}) OR name LIKE 'Total %'"
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
        args_name String'){where}
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
