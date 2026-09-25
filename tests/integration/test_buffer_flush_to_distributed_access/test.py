# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

"""
A `Buffer` flush into a `Distributed` table must not gain access the writer does not have.

A user `INSERT` into a `Distributed` table over a cluster with an interserver `<secret>` is
executed on the shard as the initiating user (`initial_user`), and the shard checks the grants of
that user. The background flush of a `Buffer` table is different: `writeBlockToDestination` runs
with a copy of the global context, which has no user. When the destination is a `Distributed`
table, the shard receives an interserver query with an empty `initial_user` and executes it with
full access. A tenant that can create tables only in its own database can therefore chain
`Buffer -> Distributed('secure', default, secret_data)` and have the flush write into a table on
the shard that the tenant has no `INSERT` grant on.

The `secure` cluster consists of `node2` only, so a table on `node1` can reach `default.secret_data`
solely over the interserver connection. Every pipeline is built twice with the same statements: by
`tenant`, who has no grants on `node2` and whose rows must never arrive, and by `writer`, who has
`INSERT` on the target and whose rows must arrive.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)

WRITER = "writer"
TENANT = "tenant"
CREDENTIALS = {
    WRITER: {"user": WRITER, "password": "wpass"},
    TENANT: {"user": TENANT, "password": "tpass"},
}
ROWS_PER_PIPELINE = 2

# How long the background flush gets before the check. The buffer is configured to flush about
# one second after the insert.
DELIVERY_WAIT_SECONDS = 30

users = pytest.mark.parametrize("user", [WRITER, TENANT])


def database(user):
    return f"{user}_db"


def query_as(user, sql):
    return node1.query(sql, **CREDENTIALS[user])


def remote_rows():
    return int(node2.query("SELECT count() FROM default.secret_data"))


def wait_until(condition, timeout=DELIVERY_WAIT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.5)
    return False


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        # The protected table lives on the shard only. Both users exist on both nodes, but on
        # the shard only `writer` may insert into it.
        node2.query(
            """
            CREATE TABLE default.secret_data (key Int) ENGINE = MergeTree ORDER BY key;
            CREATE USER writer IDENTIFIED WITH plaintext_password BY 'wpass';
            CREATE USER tenant IDENTIFIED WITH plaintext_password BY 'tpass';
            GRANT INSERT ON default.secret_data TO writer;
            """
        )
        for user in (WRITER, TENANT):
            db = database(user)
            node1.query(
                f"""
                CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{CREDENTIALS[user]["password"]}';
                CREATE DATABASE {db};
                GRANT ALL ON {db}.* TO {user};
                GRANT TABLE ENGINE ON Distributed, TABLE ENGINE ON Buffer TO {user};
                """
            )
            query_as(
                user,
                f"CREATE TABLE {db}.dist (key Int) ENGINE = Distributed(secure, default, secret_data)",
            )

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def clean_remote_table():
    node2.query("TRUNCATE TABLE default.secret_data")
    yield


@users
def test_direct_insert(user):
    """The control case: a direct `INSERT` carries the user's name to the shard, where it is checked."""
    db = database(user)
    if user == WRITER:
        query_as(WRITER, f"INSERT INTO {db}.dist VALUES (1)")
        assert remote_rows() == 1
    else:
        error = node1.query_and_get_error(
            f"INSERT INTO {db}.dist VALUES (1)", **CREDENTIALS[TENANT]
        )
        assert "ACCESS_DENIED" in error
        assert remote_rows() == 0


@users
def test_buffer_flush(user):
    db = database(user)
    # Flush by time only, about one second after the insert.
    query_as(
        user,
        f"""
        CREATE TABLE {db}.buffer (key Int)
        ENGINE = Buffer({db}, dist,
            1, /* num_layers */
            1, /* min_time */
            1, /* max_time */
            0, /* min_rows */
            1000000, /* max_rows */
            0, /* min_bytes */
            100000000 /* max_bytes */
        )
        """,
    )

    query_as(
        user,
        f"INSERT INTO {db}.buffer VALUES {', '.join(f'({i})' for i in range(ROWS_PER_PIPELINE))}",
    )

    if user == WRITER:
        assert wait_until(lambda: remote_rows() == ROWS_PER_PIPELINE)
    else:
        # Give the background flush its chance, then also flush explicitly. The explicit flush
        # is expected to fail once the flush is access-checked, hence the error is ignored.
        wait_until(lambda: remote_rows() > 0, timeout=10)
        node1.query_and_get_answer_with_error(
            f"OPTIMIZE TABLE {db}.buffer", **CREDENTIALS[TENANT]
        )
        assert remote_rows() == 0
