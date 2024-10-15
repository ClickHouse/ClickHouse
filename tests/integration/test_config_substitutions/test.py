import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", user_configs=["configs/config_no_substs.xml"]
)  # hardcoded value 33333
node2 = cluster.add_instance(
    "node2",
    user_configs=["configs/config_env.xml"],
    env_variables={"MAX_QUERY_SIZE": "55555"},
)
node3 = cluster.add_instance(
    "node3",
    user_configs=[
        "configs/config_zk.xml",
    ],
    main_configs=["configs/config_zk_include_test.xml"],
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    user_configs=["configs/config_incl.xml"],
    main_configs=["configs/include_from_source.xml"],
)  # include value 77777
node5 = cluster.add_instance(
    "node5", user_configs=["configs/config_allow_databases.xml"]
)
node6 = cluster.add_instance(
    "node6",
    user_configs=["configs/config_include_from_env.xml"],
    env_variables={
        "INCLUDE_FROM_ENV": "/etc/clickhouse-server/config.d/include_from_source.xml"
    },
    main_configs=["configs/include_from_source.xml"],
)
node7 = cluster.add_instance(
    "node7",
    user_configs=[
        "configs/000-users_with_env_subst.xml",
        "configs/010-env_subst_override.xml",
    ],
    main_configs=[
        "configs/000-server_overrides.xml",
        "configs/010-server_with_env_subst.xml",
    ],
    env_variables={
        # overridden with 424242
        "MAX_QUERY_SIZE": "121212",
        "MAX_THREADS": "2",
    },
    instance_env_variables=True,
)
node8 = cluster.add_instance(
    "node8",
    user_configs=["configs/config_include_from_yml.xml"],
    main_configs=["configs/include_from_source.yml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:

        def create_zk_roots(zk):
            zk.create(path="/setting/max_query_size", value=b"77777", makepath=True)
            zk.create(
                path="/users_from_zk_1",
                value=b"<user_1><password></password><profile>default</profile></user_1>",
                makepath=True,
            )
            zk.create(
                path="/users_from_zk_2",
                value=b"<user_2><password></password><profile>default</profile></user_2>",
                makepath=True,
            )
            zk.create(
                path="/min_bytes_for_wide_part",
                value=b"<merge_tree><min_bytes_for_wide_part>33</min_bytes_for_wide_part></merge_tree>",
                makepath=True,
            )
            zk.create(
                path="/merge_max_block_size",
                value=b"<merge_max_block_size>8888</merge_max_block_size>",
                makepath=True,
            )

        cluster.add_zookeeper_startup_command(create_zk_roots)

        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_config(start_cluster):
    assert (
        node1.query("select value from system.settings where name = 'max_query_size'")
        == "33333\n"
    )
    assert (
        node2.query("select value from system.settings where name = 'max_query_size'")
        == "55555\n"
    )
    assert (
        node3.query("select value from system.settings where name = 'max_query_size'")
        == "77777\n"
    )
    assert (
        node4.query("select value from system.settings where name = 'max_query_size'")
        == "99999\n"
    )
    assert (
        node6.query("select value from system.settings where name = 'max_query_size'")
        == "99999\n"
    )
    assert (
        node7.query("select value from system.settings where name = 'max_query_size'")
        == "424242\n"
    )
    assert (
        node7.query("select value from system.settings where name = 'max_threads'")
        == "2\n"
    )
    assert (
        node8.query("select value from system.settings where name = 'max_query_size'")
        == "88888\n"
    )


def test_config_from_env_overrides(start_cluster):
    node7.replace_config(
        "/etc/clickhouse-server/users.d/000-users_with_env_subst.xml",
        """
<clickhouse>
  <profiles>
    <default>
        <max_query_size from_env="MAX_QUERY_SIZE" />
        <max_threads from_env="MAX_THREADS">100</max_threads>
    </default>
  </profiles>
  <users>
      <default>
          <password></password>
          <profile>default</profile>
          <quota>default</quota>
      </default>

      <include incl="users_1" />
      <include incl="users_2" />
  </users>
</clickhouse>
""",
    )
    with pytest.raises(
        QueryRuntimeException,
        match="Failed to preprocess config '/etc/clickhouse-server/users.xml': Exception: Element <max_threads> has value and does not have 'replace' attribute, can't process from_env substitution",
    ):
        node7.query("SYSTEM RELOAD CONFIG")
    node7.replace_config(
        "/etc/clickhouse-server/users.d/000-users_with_env_subst.xml",
        """
<clickhouse>
  <profiles>
    <default>
        <max_query_size from_env="MAX_QUERY_SIZE" />
        <max_threads replace="1" from_env="MAX_THREADS">1</max_threads>
    </default>
  </profiles>
  <users>
      <default>
          <password></password>
          <profile>default</profile>
          <quota>default</quota>
      </default>

      <include incl="users_1" />
      <include incl="users_2" />
  </users>
</clickhouse>
""",
    )
    node7.query("SYSTEM RELOAD CONFIG")


def test_config_merge_from_env_overrides(start_cluster):
    assert (
        node7.query(
            "SELECT value FROM system.server_settings WHERE name='max_thread_pool_size'"
        )
        == "10000\n"
    )
    node7.replace_config(
        "/etc/clickhouse-server/config.d/010-server_with_env_subst.xml",
        """
<clickhouse>
    <max_thread_pool_size from_env="CH_THREADS" replace="1">9000</max_thread_pool_size>
</clickhouse>
""",
    )
    node7.query("SYSTEM RELOAD CONFIG")


def test_include_config(start_cluster):
    # <include incl="source tag" />
    assert node4.query("select 1")
    assert node4.query("select 1", user="user_1")
    assert node4.query("select 1", user="user_2")

    # <include from_zk="zk path />
    assert node3.query("select 1")
    assert node3.query("select 1", user="user_1")
    assert node3.query("select 1", user="user_2")

    # <include incl="source tag" /> from .yml source
    assert node8.query("select 1")
    assert node8.query("select 1", user="user_1")
    assert node8.query("select 1", user="user_2")


def test_allow_databases(start_cluster):
    node5.query("CREATE DATABASE db1")
    node5.query(
        "CREATE TABLE db1.test_table(date Date, k1 String, v1 Int32) ENGINE = MergeTree(date, (k1, date), 8192)"
    )
    node5.query("INSERT INTO db1.test_table VALUES('2000-01-01', 'test_key', 1)")
    assert (
        node5.query("SELECT name FROM system.databases WHERE name = 'db1'") == "db1\n"
    )
    assert (
        node5.query(
            "SELECT name FROM system.tables WHERE database = 'db1' AND name = 'test_table' "
        )
        == "test_table\n"
    )
    assert (
        node5.query(
            "SELECT name FROM system.columns WHERE database = 'db1' AND table = 'test_table'"
        )
        == "date\nk1\nv1\n"
    )
    assert (
        node5.query(
            "SELECT name FROM system.parts WHERE database = 'db1' AND table = 'test_table'"
        )
        == "20000101_20000101_1_1_0\n"
    )
    assert (
        node5.query(
            "SELECT name FROM system.parts_columns WHERE database = 'db1' AND table = 'test_table'"
        )
        == "20000101_20000101_1_1_0\n20000101_20000101_1_1_0\n20000101_20000101_1_1_0\n"
    )

    assert (
        node5.query(
            "SELECT name FROM system.databases WHERE name = 'db1'", user="test_allow"
        ).strip()
        == ""
    )
    assert (
        node5.query(
            "SELECT name FROM system.tables WHERE database = 'db1' AND name = 'test_table'",
            user="test_allow",
        ).strip()
        == ""
    )
    assert (
        node5.query(
            "SELECT name FROM system.columns WHERE database = 'db1' AND table = 'test_table'",
            user="test_allow",
        ).strip()
        == ""
    )
    assert (
        node5.query(
            "SELECT name FROM system.parts WHERE database = 'db1' AND table = 'test_table'",
            user="test_allow",
        ).strip()
        == ""
    )
    assert (
        node5.query(
            "SELECT name FROM system.parts_columns WHERE database = 'db1' AND table = 'test_table'",
            user="test_allow",
        ).strip()
        == ""
    )


def test_config_multiple_zk_substitutions(start_cluster):
    assert (
        node3.query(
            "SELECT value FROM system.merge_tree_settings WHERE name='min_bytes_for_wide_part'"
        )
        == "33\n"
    )
    assert (
        node3.query(
            "SELECT value FROM system.merge_tree_settings WHERE name='min_rows_for_wide_part'"
        )
        == "1111\n"
    )
    assert (
        node3.query(
            "SELECT value FROM system.merge_tree_settings WHERE name='merge_max_block_size'"
        )
        == "8888\n"
    )
    assert (
        node3.query(
            "SELECT value FROM system.server_settings WHERE name='background_pool_size'"
        )
        == "44\n"
    )

    zk = cluster.get_kazoo_client("zoo1")
    zk.create(
        path="/background_pool_size",
        value=b"<background_pool_size>72</background_pool_size>",
        makepath=True,
    )

    node3.replace_config(
        "/etc/clickhouse-server/config.d/config_zk_include_test.xml",
        """
<clickhouse>
  <include from_zk="/background_pool_size" merge="true"/>
  <background_pool_size>44</background_pool_size>
  <merge_tree>
    <include from_zk="/merge_max_block_size" merge="true"/>
    <min_bytes_for_wide_part>1</min_bytes_for_wide_part>
    <min_rows_for_wide_part>1111</min_rows_for_wide_part>
  </merge_tree>

  <include from_zk="/min_bytes_for_wide_part" merge="true"/>
 </clickhouse>
""",
    )

    node3.query("SYSTEM RELOAD CONFIG")

    assert (
        node3.query(
            "SELECT value FROM system.server_settings WHERE name='background_pool_size'"
        )
        == "72\n"
    )
