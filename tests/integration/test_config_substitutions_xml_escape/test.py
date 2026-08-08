"""Config substitutions with XML special characters in from_env / from_zk values.

Values substituted via `from_env` and `from_zk` used to be concatenated into an XML string
verbatim, so a value containing `&`, `<`, `>` or the `]]>` sequence failed with
`SAXParseException`. These tests cover the escaping of such values and the related from_zk
value-interpretation contract (XML fragment vs YAML subtree vs literal scalar).

The per-case values are fed through per-case *users and profiles* on a small number of
instances (instead of one instance per case) to keep the module's memory footprint low: the
integration flaky check runs several copies of the whole module concurrently, and a cluster
of many sanitizer-built servers exceeds the job's memory cgroup.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# Env var substitutions with XML special characters: one user/profile per env var.
node_env = cluster.add_instance(
    "node_env",
    user_configs=["configs/config_env_users.xml"],
    env_variables={
        "ENV_XML_SPECIAL": "a&b<c>d",
        "ENV_XML_FRAGMENT": "<a>1</a>",
        "ENV_CDATA_END": "a]]>b",
    },
)
# from_zk substitutions into user profiles (one user/profile per case), plus a from_zk
# subtree substitution into an ordinary (non-<include>) server-level element: the ZooKeeper
# node holds an XML fragment (a value beginning with '<'), so its subtree is spliced in as
# child elements of <merge_tree>. This confirms subtree substitution works on any element,
# not only a structural <include>. The config uses replace="replace" because the common
# instance config injected into every node (helpers/0_common_instance_config.xml) already
# defines <merge_tree>, so after merging the element has content and a from_zk subtree can
# only be spliced into it with "replace".
node_zk = cluster.add_instance(
    "node_zk",
    main_configs=["configs/config_zk_ordinary_xml_subtree.xml"],
    user_configs=["configs/config_zk_users.xml"],
    with_zookeeper=True,
)
# from_zk on an ordinary (non-<include>) element such as <merge_tree> with a YAML (non-'<')
# value: it must be kept as literal text, not autodetected as YAML, so the setting inside is
# NOT applied. YAML subtree autodetection is applied only to a structural <include from_zk=...>.
# This needs its own instance because node_zk's config applies the setting to the same
# <merge_tree> element, while this test asserts the setting keeps its default value.
node_zk_literal = cluster.add_instance(
    "node_zk_literal",
    main_configs=["configs/config_zk_ordinary_yaml_is_literal.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:

        def create_zk_roots(zk):
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
            # A leaf value that was XML-entity-encoded (`a&amp;b`) to satisfy the old parser, which
            # reparsed every from_zk value as XML and would have decoded this to `a&b`. A non-`<` leaf
            # value is now kept as literal text using its exact original bytes, so it resolves to the
            # literal `a&amp;b`. This is a deliberate, documented behaviour change on upgrade.
            zk.create(
                path="/leaf_entity_encoded",
                value=b"a&amp;b",
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


def get_log_comment(node, user):
    return node.query("SELECT value FROM system.settings WHERE name = 'log_comment'", user=user)


def test_config_env_xml_special_chars(start_cluster):
    """Env var values with XML special characters (&, <, >) should be auto-escaped."""
    assert get_log_comment(node_env, "env_special") == "a&b<c>d\n"


def test_config_env_xml_fragment_is_literal_text(start_cluster):
    """Env var values are always plain text: an XML-looking value must not be parsed as XML."""
    assert get_log_comment(node_env, "env_fragment") == "<a>1</a>\n"


def test_config_env_cdata_end_sequence(start_cluster):
    """An env var value containing `]]>` must be escaped, not break config parsing.

    The XML grammar forbids the literal `]]>` in character data, so embedding the value verbatim
    would yield the not-well-formed `<from_env>]]></from_env>`. The value must round-trip to its
    exact original bytes.
    """
    assert get_log_comment(node_env, "env_cdata") == "a]]>b\n"


def test_config_zk_yaml_is_autodetected(start_cluster):
    """A structural <include from_zk=...> whose value does not start with '<' is autodetected as YAML."""
    assert (
        node_zk.query(
            "SELECT value FROM system.settings WHERE name = 'max_query_size'",
            user="zk_yaml",
        )
        == "99999\n"
    )


def test_config_zk_scalar_keeps_literal_text(start_cluster):
    """A from_zk leaf scalar that contains YAML syntax must be kept as literal text.

    "abc # rotated" must not be reinterpreted by the YAML parser (which would drop the
    "# rotated" comment and yield just "abc"), so existing scalar substitutions such as
    secrets keep their exact value.
    """
    assert get_log_comment(node_zk, "zk_yaml") == "abc # rotated\n"


def test_config_zk_include_scalar_keeps_literal_text(start_cluster):
    """A plain scalar referenced via a structural <include from_zk=...> must stay literal text.

    <include from_zk=...> is not only the subtree-splicing form: it is also the generic
    "replace this element with the node contents" form, so it can sit under a leaf setting
    such as <log_comment>. A plain scalar carrying YAML syntax ("abc # rotated") must not be
    routed through the YAML parser (which would drop the "# rotated" suffix, taken as a YAML
    comment); only a value that actually is a YAML mapping or sequence is expanded as a subtree.
    """
    assert get_log_comment(node_zk, "zk_include_scalar") == "abc # rotated\n"


def test_config_zk_leaf_yaml_mapping_keeps_literal_text(start_cluster):
    """A from_zk leaf value that is a valid YAML mapping must be kept as literal text.

    "abc: def" is a valid YAML mapping, but as a leaf substitution it must stay the literal
    text "abc: def" instead of being expanded into an <abc>def</abc> sub-element. This keeps
    existing scalar settings and secrets that happen to look like YAML working unchanged.
    """
    assert get_log_comment(node_zk, "zk_leaf_mapping") == "abc: def\n"


def test_config_zk_leaf_cdata_end_sequence(start_cluster):
    """A from_zk leaf value containing `]]>` uses the same escaping helper as from_env.

    It must be kept as literal text and must not break config parsing.
    """
    assert get_log_comment(node_zk, "zk_leaf_cdata") == "a]]>b\n"


def test_config_zk_yaml_top_level_sequence_include(start_cluster):
    """A structural <include from_zk=...> whose YAML document root is a sequence must insert every item.

    A top-level sequence used to clone the synthetic `clickhouse` root, appending several root
    elements of which only the first was kept, so every item after the first was silently dropped.
    Both settings from the two-item sequence must therefore be present.
    """
    assert (
        node_zk.query(
            "SELECT value FROM system.settings WHERE name = 'max_query_size'",
            user="zk_yaml_sequence",
        )
        == "99999\n"
    )
    assert (
        node_zk.query(
            "SELECT value FROM system.settings WHERE name = 'max_result_rows'",
            user="zk_yaml_sequence",
        )
        == "12345\n"
    )


def test_config_zk_leaf_crlf_preserved(start_cluster):
    """A from_zk leaf value containing a CR/LF must survive byte-for-byte.

    XML end-of-line normalization (XML 1.0, section 2.11) rewrites `\\r\\n` to `\\n` when the
    synthetic <from_zk> document is reparsed, so the value would otherwise be silently corrupted.
    `a\\r\\nb` is 0x61 0x0D 0x0A 0x62, i.e. hex `610D0A62`.
    """
    assert (
        node_zk.query(
            "SELECT hex(value) FROM system.settings WHERE name = 'log_comment'",
            user="zk_leaf_crlf",
        )
        == "610D0A62\n"
    )


def test_config_zk_leaf_entity_encoded_stays_literal(start_cluster):
    """A from_zk leaf value that was XML-entity-encoded now resolves to its literal bytes.

    Before this change every from_zk value was reparsed as XML, so a leaf scalar stored as
    `a&amp;b` (the only way to smuggle a `&` past the old parser, which rejected a raw `&` as not
    well-formed) decoded to `a&b`. A non-`<` leaf value is now kept as literal text using its exact
    original bytes, so it resolves to the literal `a&amp;b` instead. This is a deliberate behaviour
    change on upgrade, documented in configuration-files.md: a value needing a literal `&`, `<` or
    `>` must now be stored raw rather than entity-encoded.
    """
    assert get_log_comment(node_zk, "zk_entity_encoded") == "a&amp;b\n"


def test_config_zk_ordinary_element_xml_subtree(start_cluster):
    """A from_zk value beginning with '<' is spliced as child elements into an ordinary element.

    Subtree substitution via from_zk works on any element, not only a structural <include>: an XML
    fragment stored at the ZooKeeper node becomes child elements of an ordinary container such as
    `<merge_tree from_zk=.../>`, so `min_bytes_for_wide_part` is set to 33.
    """
    assert node_zk.query("SELECT value FROM system.merge_tree_settings WHERE name = 'min_bytes_for_wide_part'") == "33\n"


def test_config_zk_ordinary_element_yaml_is_literal(start_cluster):
    """A from_zk YAML (non-'<') value on an ordinary element is kept literal, not expanded as YAML.

    YAML subtree autodetection is applied only to a structural <include from_zk=...>. On an ordinary
    element such as `<merge_tree from_zk=.../>` a non-'<' value is kept as literal text (an ordinary
    element may just as well be a leaf whose exact scalar bytes must be preserved), so the YAML
    `min_bytes_for_wide_part: 33` is NOT applied and the setting keeps its default value (not 33). To
    splice a subtree into an ordinary element, an XML fragment (a value beginning with '<') is used.
    """
    assert node_zk_literal.query("SELECT value FROM system.merge_tree_settings WHERE name = 'min_bytes_for_wide_part'") != "33\n"
