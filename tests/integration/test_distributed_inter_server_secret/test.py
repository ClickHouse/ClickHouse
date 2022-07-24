# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest
import uuid
import time

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def make_instance(name, cfg):
    return cluster.add_instance(
        name,
        with_zookeeper=True,
        main_configs=["configs/remote_servers.xml", cfg],
        user_configs=["configs/users.xml"],
    )


# _n1/_n2 contains cluster with different <secret> -- should fail
n1 = make_instance("n1", "configs/remote_servers_n1.xml")
n2 = make_instance("n2", "configs/remote_servers_n2.xml")

users = pytest.mark.parametrize(
    "user,password",
    [
        ("default", ""),
        ("nopass", ""),
        ("pass", "foo"),
    ],
)


def bootstrap():
    for n in list(cluster.instances.values()):
        n.query("DROP TABLE IF EXISTS data")
        n.query("DROP TABLE IF EXISTS data_from_buffer")
        n.query("DROP TABLE IF EXISTS dist")
        n.query("CREATE TABLE data (key Int) Engine=Memory()")
        n.query("CREATE TABLE data_from_buffer (key Int) Engine=Memory()")
        n.query(
            """
        CREATE TABLE dist_insecure AS data
        Engine=Distributed(insecure, currentDatabase(), data, key)
        """
        )
        n.query(
            """
        CREATE TABLE dist_secure AS data
        Engine=Distributed(secure, currentDatabase(), data, key)
        """
        )
        n.query(
            """
        CREATE TABLE dist_secure_from_buffer AS data_from_buffer
        Engine=Distributed(secure, currentDatabase(), data_from_buffer, key)
        """
        )
        n.query(
            """
        CREATE TABLE dist_secure_disagree AS data
        Engine=Distributed(secure_disagree, currentDatabase(), data, key)
        """
        )
        n.query(
            """
        CREATE TABLE dist_secure_buffer AS dist_secure_from_buffer
        Engine=Buffer(currentDatabase(), dist_secure_from_buffer,
            /* settings for manual flush only */
            1, /* num_layers */
            0, /* min_time, placeholder */
            0, /* max_time, placeholder */
            0, /* min_rows   */
            0, /* max_rows   */
            0, /* min_bytes  */
            0  /* max_bytes  */
        )
        """
        )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        bootstrap()
        yield cluster
    finally:
        cluster.shutdown()


def query_with_id(node, id_, query, **kwargs):
    return node.query("WITH '{}' AS __id {}".format(id_, query), **kwargs)


# @return -- [user, initial_user]
def get_query_user_info(node, query_pattern):
    node.query("SYSTEM FLUSH LOGS")
    return (
        node.query(
            """
    SELECT user, initial_user
    FROM system.query_log
    WHERE
        query LIKE '%{}%' AND
        query NOT LIKE '%system.query_log%' AND
        type = 'QueryFinish'
    """.format(
                query_pattern
            )
        )
        .strip()
        .split("\t")
    )


# @return -- [user, initial_user]
def get_query_user_info_by_id(node, query_id):
    node.query("SYSTEM FLUSH LOGS")
    return (
        node.query(
            """
    SELECT user, initial_user
    FROM system.query_log
    WHERE
        query_id = '{}' AND
        type = 'QueryFinish'
    """.format(
                query_id
            )
        )
        .strip()
        .split("\t")
    )


# @return -- settings
def get_query_setting_on_shard(node, query_pattern, setting):
    node.query("SYSTEM FLUSH LOGS")
    return node.query(
        """
    SELECT Settings['{}']
    FROM system.query_log
    WHERE
        query LIKE '%{}%' AND
        NOT is_initial_query AND
        query NOT LIKE '%system.query_log%' AND
        type = 'QueryFinish'
    LIMIT 1
    """.format(
            setting, query_pattern
        )
    ).strip()


def test_insecure():
    n1.query("SELECT * FROM dist_insecure")


def test_insecure_insert_async():
    n1.query("TRUNCATE TABLE data")
    n1.query("INSERT INTO dist_insecure SELECT * FROM numbers(2)")
    n1.query("SYSTEM FLUSH DISTRIBUTED ON CLUSTER insecure dist_insecure")
    assert int(n1.query("SELECT count() FROM dist_insecure")) == 2
    n1.query("TRUNCATE TABLE data ON CLUSTER insecure")


def test_insecure_insert_sync():
    n1.query("TRUNCATE TABLE data")
    n1.query(
        "INSERT INTO dist_insecure SELECT * FROM numbers(2)",
        settings={"insert_distributed_sync": 1},
    )
    assert int(n1.query("SELECT count() FROM dist_insecure")) == 2
    n1.query("TRUNCATE TABLE data ON CLUSTER secure")


def test_secure():
    n1.query("SELECT * FROM dist_secure")


def test_secure_insert_async():
    n1.query("TRUNCATE TABLE data")
    n1.query("INSERT INTO dist_secure SELECT * FROM numbers(2)")
    n1.query("SYSTEM FLUSH DISTRIBUTED ON CLUSTER secure dist_secure")
    assert int(n1.query("SELECT count() FROM dist_secure")) == 2
    n1.query("TRUNCATE TABLE data ON CLUSTER secure")


def test_secure_insert_sync():
    n1.query("TRUNCATE TABLE data")
    n1.query(
        "INSERT INTO dist_secure SELECT * FROM numbers(2)",
        settings={"insert_distributed_sync": 1},
    )
    assert int(n1.query("SELECT count() FROM dist_secure")) == 2
    n1.query("TRUNCATE TABLE data ON CLUSTER secure")


# INSERT w/o initial_user
#
# Buffer() flush happens with global context, that does not have user
# And so Context::user/ClientInfo::current_user/ClientInfo::initial_user will be empty
#
# This is the regression test for the subsequent query that it
# will not use user from the previous query.
#
# The test a little bit complex, but I will try to explain:
# - first, we need to execute query with the readonly user (regualar SELECT),
#   and then we will execute INSERT, and if the bug is there, then INSERT will
#   use the user from SELECT and will fail (since you cannot do INSERT with
#   readonly=1/2)
#
# - the trick with generating random priority (via sed) is to avoid reusing
#   connection from n1 to n2 from another test (and we cannot simply use
#   another pool after ConnectionPoolFactory had been added [1].
#
#     [1]: https://github.com/ClickHouse/ClickHouse/pull/26318
#
#   We need at least one change in one of fields of the node/shard definition,
#   and this "priorirty" for us in this test.
#
# - after we will ensure that connection is really established from the context
#   of SELECT query, and that the connection will not be established from the
#   context of the INSERT query (but actually it is a no-op since the INSERT
#   will be done in background, due to insert_distributed_sync=false by
#   default)
#
# - if the bug is there, then FLUSH DISTRIBUTED will fail, because it will go
#   from n1 to n2 using previous user.
#
# I hope that this will clarify something for the reader.
def test_secure_insert_buffer_async():
    # Change cluster definition so that the SELECT will always creates new connection
    priority = int(time.time())
    n1.exec_in_container(
        [
            "bash",
            "-c",
            f'sed -i "s#<priority>.*</priority>#<priority>{priority}</priority>#" /etc/clickhouse-server/config.d/remote_servers.xml',
        ]
    )
    n1.query("SYSTEM RELOAD CONFIG")
    # ensure that SELECT creates new connection (we need separate table for
    # this, so that separate distributed pool will be used)
    query_id = uuid.uuid4().hex
    n1.query("SELECT * FROM dist_secure_from_buffer", user="ro", query_id=query_id)
    assert n1.contains_in_log(
        "{" + query_id + "} <Trace> Connection (n2:9000): Connecting."
    )

    query_id = uuid.uuid4().hex
    n1.query(
        "INSERT INTO dist_secure_buffer SELECT * FROM numbers(2)", query_id=query_id
    )
    # ensure that INSERT does not creates new connection, so that it will use
    # previous connection that was instantiated with "ro" user (using
    # interserver secret)
    assert not n1.contains_in_log(
        "{" + query_id + "} <Trace> Connection (n2:9000): Connecting."
    )
    assert get_query_user_info_by_id(n1, query_id) == ["default", "default"]

    # And before the bug was fixed this query will fail with the following error:
    #
    #     Code: 164. DB::Exception: Received from 172.16.2.5:9000. DB::Exception: There was an error on [n1:9000]: Code: 164. DB::Exception: Received from n2:9000. DB::Exception: ro: Cannot execute query in readonly mode. (READONLY)
    n1.query("SYSTEM FLUSH DISTRIBUTED ON CLUSTER secure dist_secure_from_buffer")
    n1.query("OPTIMIZE TABLE dist_secure_buffer")
    n1.query("SYSTEM FLUSH DISTRIBUTED ON CLUSTER secure dist_secure_from_buffer")

    # Check user from which the INSERT on the remote node will be executed
    #
    # Incorrect example:
    #
    #    {2c55669f-71ad-48fe-98fa-7b475b80718e} <Debug> executeQuery: (from 172.16.1.1:44636, user: ro) INSERT INTO default.data_from_buffer (key) VALUES
    #
    # Correct example:
    #
    #    {2c55669f-71ad-48fe-98fa-7b475b80718e} <Debug> executeQuery: (from 0.0.0.0:0, user: ) INSERT INTO default.data_from_buffer (key) VALUES
    #
    assert n2.contains_in_log(
        "executeQuery: (from 0.0.0.0:0, user: ) INSERT INTO default.data_from_buffer (key) VALUES"
    )

    assert int(n1.query("SELECT count() FROM dist_secure_from_buffer")) == 2
    n1.query("TRUNCATE TABLE data_from_buffer ON CLUSTER secure")


def test_secure_disagree():
    with pytest.raises(QueryRuntimeException, match=".*Hash mismatch.*"):
        n1.query("SELECT * FROM dist_secure_disagree")


def test_secure_disagree_insert():
    n1.query("TRUNCATE TABLE data")
    n1.query("INSERT INTO dist_secure_disagree SELECT * FROM numbers(2)")
    with pytest.raises(QueryRuntimeException, match=".*Hash mismatch.*"):
        n1.query(
            "SYSTEM FLUSH DISTRIBUTED ON CLUSTER secure_disagree dist_secure_disagree"
        )
    # check the the connection will be re-established
    # IOW that we will not get "Unknown BlockInfo field"
    with pytest.raises(QueryRuntimeException, match=".*Hash mismatch.*"):
        assert int(n1.query("SELECT count() FROM dist_secure_disagree")) == 0


@users
def test_user_insecure_cluster(user, password):
    id_ = "query-dist_insecure-" + user
    query_with_id(n1, id_, "SELECT * FROM dist_insecure", user=user, password=password)
    assert get_query_user_info(n1, id_) == [
        user,
        user,
    ]  # due to prefer_localhost_replica
    assert get_query_user_info(n2, id_) == ["default", user]


@users
def test_user_secure_cluster(user, password):
    id_ = "query-dist_secure-" + user
    query_with_id(n1, id_, "SELECT * FROM dist_secure", user=user, password=password)
    assert get_query_user_info(n1, id_) == [user, user]
    assert get_query_user_info(n2, id_) == [user, user]


@users
def test_per_user_inline_settings_insecure_cluster(user, password):
    id_ = "query-ddl-settings-dist_insecure-" + user
    query_with_id(
        n1,
        id_,
        """
    SELECT * FROM dist_insecure
    SETTINGS
        prefer_localhost_replica=0,
        max_memory_usage_for_user=1e9,
        max_untracked_memory=0
    """,
        user=user,
        password=password,
    )
    assert get_query_setting_on_shard(n1, id_, "max_memory_usage_for_user") == ""


@users
def test_per_user_inline_settings_secure_cluster(user, password):
    id_ = "query-ddl-settings-dist_secure-" + user
    query_with_id(
        n1,
        id_,
        """
    SELECT * FROM dist_secure
    SETTINGS
        prefer_localhost_replica=0,
        max_memory_usage_for_user=1e9,
        max_untracked_memory=0
    """,
        user=user,
        password=password,
    )
    assert int(get_query_setting_on_shard(n1, id_, "max_memory_usage_for_user")) == int(
        1e9
    )


@users
def test_per_user_protocol_settings_insecure_cluster(user, password):
    id_ = "query-protocol-settings-dist_insecure-" + user
    query_with_id(
        n1,
        id_,
        "SELECT * FROM dist_insecure",
        user=user,
        password=password,
        settings={
            "prefer_localhost_replica": 0,
            "max_memory_usage_for_user": int(1e9),
            "max_untracked_memory": 0,
        },
    )
    assert get_query_setting_on_shard(n1, id_, "max_memory_usage_for_user") == ""


@users
def test_per_user_protocol_settings_secure_cluster(user, password):
    id_ = "query-protocol-settings-dist_secure-" + user
    query_with_id(
        n1,
        id_,
        "SELECT * FROM dist_secure",
        user=user,
        password=password,
        settings={
            "prefer_localhost_replica": 0,
            "max_memory_usage_for_user": int(1e9),
            "max_untracked_memory": 0,
        },
    )
    assert int(get_query_setting_on_shard(n1, id_, "max_memory_usage_for_user")) == int(
        1e9
    )
