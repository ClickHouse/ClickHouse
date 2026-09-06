"""Export of the `system.*_log` tables of a local server to the CI Logs cluster.

Every check exports its system logs the same way: for each system log table a
`Distributed` table (`system.<table>_sender`) is created next to it, fed by a
materialized view (`system.<table>_watcher`) that adds the columns identifying
the CI run. The rows are then sent to the CI Logs cluster in the background, by
the server itself. The tables and the views are created by
`ci/jobs/scripts/functional_tests/setup_log_cluster.sh`; this module holds what
that script needs from the job side - the cluster definition written into the
server config before it starts, the credentials, and the expression for the
extra columns - so that every job sets the export up in the same way.

A job that runs its server on a non-default port passes it as `port` (the
performance comparison runs two servers side by side), and a job that runs more
than one server tells them apart with `check_name_suffix`.
"""

import os
from pathlib import Path

from ci.jobs.scripts.log_cluster import LogCluster
from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.utils import Shell, Utils

# The cluster the `_sender` tables point at: a single replica, the CI Logs
# cluster. Defined in the server config written by `create_config`.
CLICKHOUSE_CI_LOGS_CLUSTER = "system_logs_export"
CLICKHOUSE_CI_LOGS_USER = "ci"

LOG_EXPORT_CONFIG_TEMPLATE = """
remote_servers:
    {CLICKHOUSE_CI_LOGS_CLUSTER}:
        shard:
            replica:
                secure: 1
                user: '{CLICKHOUSE_CI_LOGS_USER}'
                host: '{CLICKHOUSE_CI_LOGS_HOST}'
                port: 9440
                password: '{CLICKHOUSE_CI_LOGS_PASSWORD}'
"""

SETUP_SCRIPT = "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh"

# The `_watcher` views are created with `DEFINER = ci_logs_sender`, so that user
# must exist on the server. Jobs that install the standard test configs
# (`tests/config/install.sh`) already have it; the others copy this file.
CI_LOGS_SENDER_USER_CONFIG = "./tests/config/users.d/ci_logs_sender.yaml"

# The port of the local server to export from, for `setup_log_cluster.sh`.
SERVER_PORT_ENV = "LOG_EXPORT_SERVER_PORT"

# The `Distributed` tables the export sends through, created by `start` as
# `system.<log table>_sender`. `endsWith` rather than `LIKE '%\_sender'`, to
# keep the escape of the underscore out of a query that goes through a shell
# command line.
SENDER_TABLES_QUERY = (
    "SELECT database || '.' || name FROM system.tables "
    "WHERE database = 'system' AND endsWith(name, '_sender') AND engine = 'Distributed'"
)

_credentials = None


def get_credentials():
    """The host and the password of the CI Logs cluster, from AWS SSM.

    Fetched once per process: every server of the job exports to the same
    cluster.
    """
    global _credentials
    if _credentials is None:
        _credentials = (
            Secret.Config(
                name="clickhouse_ci_logs_host",
                type=Secret.Type.AWS_SSM_PARAMETER,
                region="us-east-1",
            )
            .join_with(
                Secret.Config(
                    name="clickhouse_ci_logs_password",
                    type=Secret.Type.AWS_SSM_PARAMETER,
                    region="us-east-1",
                )
            )
            .get_value()
        )
    return _credentials


def create_config(config_dir, host, password, users_dir=""):
    """Write the CI Logs cluster definition into the server's `config.d`.

    Must run before the server is started: without the cluster the `_sender`
    tables cannot be created (`Code: 701. Requested cluster
    'system_logs_export' not found`). `users_dir` is for the jobs that do not
    install the standard test configs and need the `ci_logs_sender` user.
    """
    print(f"Create log export config in [{config_dir}]")
    config_file = Path(config_dir) / "config.d" / "system_logs_export.yaml"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.write_text(
        LOG_EXPORT_CONFIG_TEMPLATE.format(
            CLICKHOUSE_CI_LOGS_CLUSTER=CLICKHOUSE_CI_LOGS_CLUSTER,
            CLICKHOUSE_CI_LOGS_HOST=host,
            CLICKHOUSE_CI_LOGS_USER=CLICKHOUSE_CI_LOGS_USER,
            CLICKHOUSE_CI_LOGS_PASSWORD=password,
        )
    )
    if users_dir:
        return Shell.check(
            f"mkdir -p {users_dir} && cp {CI_LOGS_SENDER_USER_CONFIG} {users_dir}/",
            verbose=True,
        )
    return True


def _set_server_port(port):
    if port:
        os.environ[SERVER_PORT_ENV] = str(port)
    else:
        os.environ.pop(SERVER_PORT_ENV, None)
    return f"--port {port}" if port else ""


def start(
    check_start_time,
    host="",
    password="",
    port=None,
    check_name_suffix="",
    commit_sha="",
):
    """Create the destination tables on the CI Logs cluster and the local views
    that feed them."""
    print(f"Start log export{f' of the server on port {port}' if port else ''}")
    if host:
        os.environ["CLICKHOUSE_CI_LOGS_CLUSTER"] = CLICKHOUSE_CI_LOGS_CLUSTER
        os.environ["CLICKHOUSE_CI_LOGS_HOST"] = host
        os.environ["CLICKHOUSE_CI_LOGS_USER"] = CLICKHOUSE_CI_LOGS_USER
        os.environ["CLICKHOUSE_CI_LOGS_PASSWORD"] = password
    # The exported columns are defined once in LogCluster.META_COLUMNS so the
    # DDL of the destination tables and these SELECT expressions cannot drift.
    check_name = Info().job_name + check_name_suffix
    os.environ["EXTRA_COLUMNS"] = LogCluster.extra_columns_ddl()
    os.environ["EXTRA_COLUMNS_EXPRESSION"] = LogCluster.extra_columns_expression(
        Utils.timestamp_to_str(check_start_time),
        check_name=check_name,
        commit_sha=commit_sha,
    )
    _set_server_port(port)

    return Shell.check(
        f"{SETUP_SCRIPT} --setup-logs-replication",
        verbose=True,
    )


def stop(port=None):
    """Flush everything that is still local and drop the views.

    The last system log records are flushed into the log tables first, and the
    rows the views push from there go through the asynchronous insert queue
    (the `ci_logs_sender` profile), so that queue is flushed as well - whatever
    is still in it when the `_sender` tables below are flushed and dropped is
    lost.
    """
    print(f"Stop log export{f' of the server on port {port}' if port else ''}")
    port_arg = _set_server_port(port)
    Shell.check(
        f'clickhouse-client {port_arg} --query "SYSTEM FLUSH LOGS"',
        verbose=True,
    )
    Shell.check(
        f'clickhouse-client {port_arg} --query "SYSTEM FLUSH ASYNC INSERT QUEUE"',
        verbose=True,
    )
    return Shell.check(
        f"{SETUP_SCRIPT} --stop-log-replication",
        verbose=True,
    )


def _switch_distributed_sends(start_sending, port):
    """`SYSTEM START|STOP DISTRIBUTED SENDS` for the export tables only.

    Without a table the statement takes the lock for every `Distributed` table
    of every database on the server, and the tables of the job are not ours to
    hold back: the performance comparison runs tests that create `Distributed`
    tables of their own (`tests/performance/add_distinct_to_in_clause.xml`).
    Only the `_sender` tables of the export are named here, so the lock cannot
    reach anything else.

    Returns whether every one of them was switched. The export tables are the
    ones created by `start`, so this must run after it.
    """
    action = "START" if start_sending else "STOP"
    port_arg = _set_server_port(port)
    tables = Shell.get_output(
        f'clickhouse-client {port_arg} --query "{SENDER_TABLES_QUERY}"',
        verbose=True,
    ).split()
    if not tables:
        # Either the listing itself failed, or the export created no table.
        print(f"WARNING: No log export table to {action} the distributed sends of")
        return False
    ok = True
    for table in tables:
        # Every table is switched, whatever happened to the previous one: a
        # table left in the other state is what this reports as a failure, and
        # stopping halfway would leave more of them.
        switched = Shell.check(
            f'clickhouse-client {port_arg} --query "SYSTEM {action} DISTRIBUTED SENDS {table}"',
            verbose=True,
        )
        ok = ok and switched
    return ok


def stop_distributed_sends(port=None):
    """Keep the exported rows in the local files of the `_sender` tables
    instead of sending them to the CI Logs cluster.

    For the performance comparison, which must not spend the measured time on
    anything else: the rows are accumulated locally while the queries are
    measured, and `start_distributed_sends` + `stop` push them afterwards.
    """
    return _switch_distributed_sends(False, port)


def start_distributed_sends(port=None):
    """Undo `stop_distributed_sends`."""
    return _switch_distributed_sends(True, port)
