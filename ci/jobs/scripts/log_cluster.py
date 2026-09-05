import time
import traceback
from dataclasses import dataclass

import requests

from ci.praktika.info import Info
from ci.praktika.secret import Secret
from ci.praktika.utils import Utils


@dataclass(frozen=True)
class MetaColumn:
    """One CI metadata column prepended to every row exported to LogCluster.

    `cast` renders the value where the destination column is created from this
    very definition (the `system.*_log` export), `literal` where the
    destination table already declares the type.
    """

    name: str
    type: str
    cast: str = "'{}'"
    literal: str = "'{}'"
    index: str = ""


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

    # The CI metadata every export to this cluster carries, defined once so
    # that the DDL of the destination table, the INSERT column list and the
    # values cannot disagree in name, order or type.
    #
    # A new column reaches the `system.*_log` tables on its own: their names
    # embed a hash of the structure, so the next job creates fresh ones. The
    # tables of LogClusterBuildProfileQueries are hand-made, so add the column
    # there with `ALTER TABLE` before merging, otherwise every upload fails.
    META_COLUMNS = (
        MetaColumn(
            name="repo",
            type="LowCardinality(String)",
            cast="toLowCardinality('{}')",
            index="INDEX ix_repo (repo) TYPE set(100)",
        ),
        MetaColumn(
            name="pull_request_number",
            type="UInt32",
            cast="CAST({} AS UInt32)",
            literal="{}",
            index="INDEX ix_pr (pull_request_number) TYPE set(100)",
        ),
        MetaColumn(
            name="commit_sha",
            type="String",
            index="INDEX ix_commit (commit_sha) TYPE set(100)",
        ),
        MetaColumn(
            name="check_start_time",
            type="DateTime('UTC')",
            cast="toDateTime('{}', 'UTC')",
            index="INDEX ix_check_time (check_start_time) TYPE minmax",
        ),
        MetaColumn(
            name="check_name",
            type="LowCardinality(String)",
            cast="toLowCardinality('{}')",
        ),
        MetaColumn(
            name="instance_type",
            type="LowCardinality(String)",
            cast="toLowCardinality('{}')",
        ),
        MetaColumn(name="instance_id", type="String"),
        MetaColumn(
            name="workflow_start_time",
            type="DateTime('UTC')",
            cast="toDateTime('{}', 'UTC')",
        ),
    )

    @classmethod
    def meta_columns(cls):
        return cls.META_COLUMNS

    @classmethod
    def workflow_start_time(cls):
        """Start of the workflow this job belongs to, as a UTC datetime string.

        `Info().workflow_start_time` is GitHub's `created_at` of the run
        (`2026-08-14T17:01:52Z`), resolved by the config job: the same value
        for every job of the run, and a rerun keeps it. Grouping rows by it
        therefore reconstructs one workflow run.
        """
        return Utils.gh_str_to_datetime(Info().workflow_start_time).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    @classmethod
    def meta_values(cls, check_start_time, check_name="", commit_sha=""):
        """Value of every metadata column for the current job.

        `check_start_time` is a UTC datetime string. `check_name` overrides the
        job name where one job uploads on behalf of several checks, as the
        build profile hook does for each build variant. `commit_sha` overrides
        the job's own sha where a job exports on behalf of another commit.
        """
        info = Info()
        return {
            "repo": info.repo_name,
            "pull_request_number": info.pr_number,
            "commit_sha": commit_sha or info.sha,
            "check_start_time": check_start_time,
            "check_name": check_name or info.job_name,
            "instance_type": info.instance_type,
            "instance_id": info.instance_id,
            "workflow_start_time": cls.workflow_start_time(),
        }

    @classmethod
    def extra_columns_ddl(cls):
        """`EXTRA_COLUMNS` for setup_log_cluster.sh: the column definitions
        followed by their skip indexes. The trailing separator belongs to it,
        the script splices the fragment right after the opening parenthesis of
        a `SHOW CREATE TABLE` output."""
        columns = cls.meta_columns()
        return "".join(
            [f"{c.name} {c.type}, " for c in columns]
            + [f"{c.index}, " for c in columns if c.index]
        )

    @classmethod
    def extra_columns_expression(cls, check_start_time, check_name="", commit_sha=""):
        """`EXTRA_COLUMNS_EXPRESSION` for setup_log_cluster.sh: the same
        columns as SELECT expressions, in the same order as the DDL above."""
        values = cls.meta_values(check_start_time, check_name, commit_sha)
        return ", ".join(
            f"{c.cast.format(values[c.name])} AS {c.name}" for c in cls.meta_columns()
        )

    @classmethod
    def meta_column_names(cls):
        return [c.name for c in cls.meta_columns()]

    @classmethod
    def meta_column_literals(cls, check_start_time, check_name=""):
        values = cls.meta_values(check_start_time, check_name)
        return [c.literal.format(values[c.name]) for c in cls.meta_columns()]

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
        post_attempted = False
        for retry in range(retries):
            # is_ready is a cheap `SELECT 1` against the same writer endpoint,
            # so it fails during the same pressure spikes as the INSERT itself.
            # Probing it once outside the loop would defeat the retries: a
            # single transient Code 241 on the probe would drop the upload
            # before any POST is attempted. Retry it on the same schedule as
            # `select` below does.
            if not self.is_ready():
                print("WARNING: LogCluster not ready")
                time.sleep(5 * (retry + 1))
                continue
            if not self._session:
                self._session = requests.Session()
            # A retry re-sends the whole body. requests consumes a file-like
            # body on the first attempt, and re-posting the exhausted stream
            # would run an INSERT with an empty body: it succeeds and the
            # telemetry is silently lost. Rewind it before every attempt.
            if hasattr(data, "seek"):
                data.seek(0)
            try:
                post_attempted = True
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
                    # A retryable error: the shared cluster goes through
                    # minutes-long server-wide memory-pressure spikes (Code 241
                    # for every query), the same ones `select` below rides out
                    # on this schedule.
                    time.sleep(5 * (retry + 1))
                    continue
                else:
                    break
            except Exception:
                print("WARNING: LogCluster query failed with exception")
                traceback.print_exc()
                time.sleep(5 * (retry + 1))
        if response is not None:
            print(
                f"ERROR: Failed to query LogCluster, query:\n {query}\n    reason:\n {response.text}"
            )
        elif post_attempted:
            # The endpoint was ready but every POST raised before returning a
            # response (timeout, connection reset, ...). Blaming readiness here
            # would point the incident at the wrong subsystem; the tracebacks
            # of the attempts are already in the log above.
            print("ERROR: Every LogCluster POST attempt failed with an exception")
        else:
            # Every attempt gave up before its POST: the endpoint never became
            # ready. Say so, otherwise the caller's fail-close assert reports a
            # lost upload with no reason in the log.
            print("ERROR: LogCluster not ready")
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

    def _columns(self, table_columns):
        names = LogCluster.meta_column_names() + list(table_columns)
        return ",\n".join(f"        {name}" for name in names)

    def _values(self, build_name, start_time):
        return ", ".join(
            LogCluster.meta_column_literals(start_time, check_name=build_name)
        )

    def insert_profile_data(self, build_name, start_time, file, reduced=False):
        query = self._profile_query(build_name, start_time, reduced=reduced)
        with open(file, "rb") as data_fd:
            assert self._log_cluster.do_query(
                query, data=data_fd, retries=8, timeout=50
            )

    def insert_build_size_data(self, build_name, start_time, file):
        query = self._build_size_query(build_name, start_time)
        with open(file, "rb") as data_fd:
            assert self._log_cluster.do_query(
                query, data=data_fd, retries=8, timeout=50
            )

    def insert_binary_symbol_data(self, build_name, start_time, file):
        query = self._binary_symbol_query(build_name, start_time)
        with open(file, "rb") as data_fd:
            assert self._log_cluster.do_query(
                query, data=data_fd, retries=8, timeout=50
            )

    def _profile_query(self, build_name, start_time, reduced=False):
        where = ""
        if reduced:
            names = ", ".join(f"'{name}'" for name in self.REDUCED_PROFILE_EVENTS)
            where = f"\n    WHERE name IN ({names}) OR name LIKE 'Total %'"
        columns = self._columns(
            (
                "file",
                "library",
                "time",
                "pid",
                "tid",
                "ph",
                "ts",
                "dur",
                "cat",
                "name",
                "detail",
                "count",
                "avgMs",
                "args_name",
            )
        )
        return f"""INSERT INTO build_time_trace
    (
{columns}
    )
    SELECT {self._values(build_name, start_time)}, *
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
        columns = self._columns(("file", "size"))
        return f"""INSERT INTO binary_sizes
    (
{columns}
    )
    SELECT {self._values(build_name, start_time)}, file, size
    FROM input('size UInt64, file String')
    SETTINGS format_regexp = '^\\s*(\\d+) (.+)$'
    FORMAT Regexp"""

    def _binary_symbol_query(self, build_name, start_time):
        columns = self._columns(("file", "address", "size", "type", "symbol"))
        return f"""INSERT INTO binary_symbols
    (
{columns}
    )
    SELECT {self._values(build_name, start_time)},
    file, reinterpretAsUInt64(reverse(unhex(address))), reinterpretAsUInt64(reverse(unhex(size))), type, symbol
    FROM input('file String, address String, size String, type String, symbol String')
    SETTINGS format_regexp = '^([^ ]+) ([0-9a-fA-F]+)(?: ([0-9a-fA-F]+))? (.) (.+)$'
    FORMAT Regexp"""


if __name__ == "__main__":
    LogCluster = LogCluster()
    assert LogCluster.is_ready()
