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
    main_configs=[
        "configs/config_zk_include_test.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
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
# env var with XML special characters (should be auto-escaped).
# `instance_env_variables=True` keeps `LOG_COMMENT_VALUE` exclusive to this node: env variables are
# otherwise shared across the whole cluster, so node9 and node10 (same variable name) would collide.
node9 = cluster.add_instance(
    "node9",
    user_configs=["configs/config_env_xml_chars.xml"],
    env_variables={"LOG_COMMENT_VALUE": "a&b<c>d"},
    instance_env_variables=True,
)
# env var that looks like an XML fragment: it must be taken as literal text, not parsed as XML
node10 = cluster.add_instance(
    "node10",
    user_configs=["configs/config_env_xml_chars.xml"],
    env_variables={"LOG_COMMENT_VALUE": "<a>1</a>"},
    instance_env_variables=True,
)
# from_zk value that is a YAML subtree, referenced from a structural <include>: it must be
# autodetected as YAML and expanded. A leaf scalar with YAML comment syntax must stay literal.
node11 = cluster.add_instance(
    "node11",
    user_configs=["configs/config_zk_yaml.xml"],
    with_zookeeper=True,
)
# from_zk value that is a valid YAML mapping ("abc: def"), but used as a leaf substitution:
# it must be kept as literal text, not expanded into an <abc>def</abc> sub-element.
node12 = cluster.add_instance(
    "node12",
    user_configs=["configs/config_zk_yaml_leaf.xml"],
    with_zookeeper=True,
)
# env var whose value contains the `]]>` sequence: the XML grammar forbids `]]>` in character
# data, so it must be escaped (as `]]&gt;`) rather than embedded verbatim, otherwise the synthetic
# document `<from_env>]]></from_env>` would be rejected as not well-formed before substitution.
node13 = cluster.add_instance(
    "node13",
    user_configs=["configs/config_env_xml_chars.xml"],
    env_variables={"LOG_COMMENT_VALUE": "a]]>b"},
    instance_env_variables=True,
)
# from_zk leaf value that contains the `]]>` sequence: it uses the same escaping helper as
# from_env, so it must be kept as literal text and not break config parsing.
node14 = cluster.add_instance(
    "node14",
    user_configs=["configs/config_zk_leaf_cdata.xml"],
    with_zookeeper=True,
)
# from_zk structural <include> whose YAML document root is a sequence of two mappings: both
# items must be inserted, not just the first.
node15 = cluster.add_instance(
    "node15",
    user_configs=["configs/config_zk_yaml_sequence.xml"],
    with_zookeeper=True,
)
# from_zk leaf value that contains a CR/LF: it must survive byte-for-byte.
node16 = cluster.add_instance(
    "node16",
    user_configs=["configs/config_zk_leaf_crlf.xml"],
    with_zookeeper=True,
)
# from_zk subtree substitution into an ordinary (non-<include>) element such as <merge_tree>:
# a value beginning with '<' is an XML fragment, so its subtree is spliced in as child elements.
# This confirms subtree substitution works on any element, not only a structural <include>.
node17 = cluster.add_instance(
    "node17",
    main_configs=["configs/config_zk_ordinary_xml_subtree.xml"],
    with_zookeeper=True,
)
# from_zk on an ordinary (non-<include>) element such as <merge_tree> with a YAML (non-'<') value:
# it must be kept as literal text, not autodetected as YAML, so the setting inside is NOT applied.
# YAML subtree autodetection is applied only to a structural <include from_zk=...>.
node18 = cluster.add_instance(
    "node18",
    main_configs=["configs/config_zk_ordinary_yaml_is_literal.xml"],
    with_zookeeper=True,
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
            # A YAML subtree (does not start with '<') stored in a ZooKeeper node: it must
            # be autodetected and parsed as YAML, just like a config file.
            zk.create(
                path="/profile_settings_yaml",
                value=b"max_query_size: 99999\n",
                makepath=True,
            )
            # A plain scalar that happens to contain YAML syntax ('#' starts a YAML comment):
            # it must be kept as literal text, not reinterpreted by the YAML parser (which would
            # otherwise turn "abc # rotated" into "abc").
            zk.create(
                path="/scalar_with_yaml_syntax",
                value=b"abc # rotated",
                makepath=True,
            )
            # A value that is a valid YAML mapping ("abc: def"), used as a leaf substitution:
            # it must be kept as literal text, not expanded into an <abc>def</abc> sub-element,
            # so an existing scalar setting or secret is preserved unchanged on upgrade.
            zk.create(
                path="/leaf_yaml_mapping",
                value=b"abc: def",
                makepath=True,
            )
            # A leaf value containing the `]]>` sequence, which the XML grammar forbids in
            # character data: it must be escaped and kept as literal text, not break parsing.
            zk.create(
                path="/leaf_with_cdata_end",
                value=b"a]]>b",
                makepath=True,
            )
            # A YAML document whose root is a *sequence* of two mappings, referenced from a
            # structural <include from_zk=...>: every item must be spliced under one synthetic
            # root, so both settings survive (a top-level sequence used to clone the synthetic
            # `clickhouse` root and keep only the first item).
            zk.create(
                path="/profile_settings_yaml_sequence",
                value=b"- max_query_size: 99999\n- max_result_rows: 12345\n",
                makepath=True,
            )
            # A leaf value that contains a CR/LF (`a\r\nb`): it must survive byte-for-byte. XML
            # end-of-line normalization would otherwise rewrite `\r\n` to `\n` when the synthetic
            # <from_zk> document is reparsed.
            zk.create(
                path="/leaf_with_crlf",
                value=b"a\r\nb",
                makepath=True,
            )
            # An XML fragment (begins with '<'), referenced from an ordinary (non-<include>) element
            # `<merge_tree from_zk=.../>`: it is spliced in as child elements, so the subtree
            # substitution applies just like it does for a structural <include>.
            zk.create(
                path="/merge_tree_xml_subtree",
                value=b"<min_bytes_for_wide_part>33</min_bytes_for_wide_part>",
                makepath=True,
            )
            # A YAML subtree (does not begin with '<'), referenced from an ordinary (non-<include>)
            # element `<merge_tree from_zk=.../>`: it must be kept as literal text, not autodetected
            # as YAML, so the `min_bytes_for_wide_part` setting inside must NOT be applied. YAML
            # autodetection is reserved for a structural <include from_zk=...>.
            zk.create(
                path="/merge_tree_yaml_subtree",
                value=b"min_bytes_for_wide_part: 33\n",
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
    with node7.with_replace_config(
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
    ):
        with pytest.raises(
            QueryRuntimeException,
            match="Failed to preprocess config '/etc/clickhouse-server/users.xml': Exception: Element <max_threads> has value and does not have 'replace' attribute, can't process from_env substitution",
        ):
            node7.query("SYSTEM RELOAD CONFIG")

    with node7.with_replace_config(
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
    ):
        node7.query("SYSTEM RELOAD CONFIG")


def test_config_merge_from_env_overrides(start_cluster):
    node7.query("SYSTEM RELOAD CONFIG")
    assert (
        node7.query(
            "SELECT value FROM system.server_settings WHERE name='max_thread_pool_size'"
        )
        == "1000\n"
    )
    with node7.with_replace_config(
        "/etc/clickhouse-server/config.d/010-server_with_env_subst.xml",
        """
<clickhouse>
    <max_thread_pool_size from_env="CH_THREADS" replace="1">9000</max_thread_pool_size>
</clickhouse>
""",
    ):
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
    node5.query("DROP DATABASE IF EXISTS db1 SYNC")
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
    # NOTE: background_pool_size cannot be decreased, so let's restart ClickHouse to make the test idempotent (i.e. runned multiple times)
    node3.restart_clickhouse()
    node3.query("SYSTEM RELOAD CONFIG")
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
    try:
        zk.create(
            path="/background_pool_size",
            value=b"<background_pool_size>72</background_pool_size>",
            makepath=True,
        )

        with node3.with_replace_config(
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
        ):
            node3.query("SYSTEM RELOAD CONFIG")

            assert (
                node3.query(
                    "SELECT value FROM system.server_settings WHERE name='background_pool_size'"
                )
                == "72\n"
            )
    finally:
        zk.delete(path="/background_pool_size")


def test_config_env_xml_special_chars(start_cluster):
    """Env var values with XML special characters (&, <, >) should be auto-escaped."""
    assert (
        node9.query(
            "SELECT value FROM system.settings WHERE name = 'log_comment'"
        )
        == "a&b<c>d\n"
    )


def test_config_env_xml_fragment_is_literal_text(start_cluster):
    """Env var values are always plain text: an XML-looking value must not be parsed as XML."""
    assert (
        node10.query(
            "SELECT value FROM system.settings WHERE name = 'log_comment'"
        )
        == "<a>1</a>\n"
    )


def test_config_zk_yaml_is_autodetected(start_cluster):
    """A structural <include from_zk=...> whose value does not start with '<' is autodetected as YAML."""
    assert (
        node11.query("SELECT value FROM system.settings WHERE name = 'max_query_size'")
        == "99999\n"
    )


def test_config_zk_scalar_keeps_literal_text(start_cluster):
    """A from_zk leaf scalar that contains YAML syntax must be kept as literal text.

    "abc # rotated" must not be reinterpreted by the YAML parser (which would drop the
    "# rotated" comment and yield just "abc"), so existing scalar substitutions such as
    secrets keep their exact value.
    """
    assert (
        node11.query("SELECT value FROM system.settings WHERE name = 'log_comment'")
        == "abc # rotated\n"
    )


def test_config_zk_leaf_yaml_mapping_keeps_literal_text(start_cluster):
    """A from_zk leaf value that is a valid YAML mapping must be kept as literal text.

    "abc: def" is a valid YAML mapping, but as a leaf substitution it must stay the literal
    text "abc: def" instead of being expanded into an <abc>def</abc> sub-element. This keeps
    existing scalar settings and secrets that happen to look like YAML working unchanged.
    """
    assert (
        node12.query("SELECT value FROM system.settings WHERE name = 'log_comment'")
        == "abc: def\n"
    )


def test_config_env_cdata_end_sequence(start_cluster):
    """An env var value containing `]]>` must be escaped, not break config parsing.

    The XML grammar forbids the literal `]]>` in character data, so embedding the value verbatim
    would yield the not-well-formed `<from_env>]]></from_env>`. The value must round-trip to its
    exact original bytes.
    """
    assert (
        node13.query("SELECT value FROM system.settings WHERE name = 'log_comment'")
        == "a]]>b\n"
    )


def test_config_zk_leaf_cdata_end_sequence(start_cluster):
    """A from_zk leaf value containing `]]>` uses the same escaping helper as from_env.

    It must be kept as literal text and must not break config parsing.
    """
    assert (
        node14.query("SELECT value FROM system.settings WHERE name = 'log_comment'")
        == "a]]>b\n"
    )


def test_config_zk_yaml_top_level_sequence_include(start_cluster):
    """A structural <include from_zk=...> whose YAML document root is a sequence must insert every item.

    A top-level sequence used to clone the synthetic `clickhouse` root, appending several root
    elements of which only the first was kept, so every item after the first was silently dropped.
    Both settings from the two-item sequence must therefore be present.
    """
    assert (
        node15.query("SELECT value FROM system.settings WHERE name = 'max_query_size'")
        == "99999\n"
    )
    assert (
        node15.query("SELECT value FROM system.settings WHERE name = 'max_result_rows'")
        == "12345\n"
    )


def test_config_zk_leaf_crlf_preserved(start_cluster):
    """A from_zk leaf value containing a CR/LF must survive byte-for-byte.

    XML end-of-line normalization (XML 1.0, section 2.11) rewrites `\\r\\n` to `\\n` when the
    synthetic <from_zk> document is reparsed, so the value would otherwise be silently corrupted.
    `a\\r\\nb` is 0x61 0x0D 0x0A 0x62, i.e. hex `610D0A62`.
    """
    assert (
        node16.query("SELECT hex(value) FROM system.settings WHERE name = 'log_comment'")
        == "610D0A62\n"
    )


def test_config_zk_ordinary_element_xml_subtree(start_cluster):
    """A from_zk value beginning with '<' is spliced as child elements into an ordinary element.

    Subtree substitution via from_zk works on any element, not only a structural <include>: an XML
    fragment stored at the ZooKeeper node becomes child elements of an ordinary container such as
    `<merge_tree from_zk=.../>`, so `min_bytes_for_wide_part` is set to 33.
    """
    assert (
        node17.query(
            "SELECT value FROM system.merge_tree_settings WHERE name = 'min_bytes_for_wide_part'"
        )
        == "33\n"
    )


def test_config_zk_ordinary_element_yaml_is_literal(start_cluster):
    """A from_zk YAML (non-'<') value on an ordinary element is kept literal, not expanded as YAML.

    YAML subtree autodetection is applied only to a structural <include from_zk=...>. On an ordinary
    element such as `<merge_tree from_zk=.../>` a non-'<' value is kept as literal text (an ordinary
    element may just as well be a leaf whose exact scalar bytes must be preserved), so the YAML
    `min_bytes_for_wide_part: 33` is NOT applied and the setting keeps its default value (not 33). To
    splice a subtree into an ordinary element, an XML fragment (a value beginning with '<') is used.
    """
    assert (
        node18.query(
            "SELECT value FROM system.merge_tree_settings WHERE name = 'min_bytes_for_wide_part'"
        )
        != "33\n"
    )
