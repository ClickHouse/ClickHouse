import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The server-level <compression> selector chooses the default codec of every part. That codec is
# fed raw (without a column type) into the statistics and text-index streams, so a codec that
# requires a column type (e.g. PCO) must be rejected when the configuration is loaded. An
# experimental codec (e.g. ZXC) is rejected as well, unless the server-level
# `enable_zxc_codec` policy (the default profile) enables it. That policy is consulted at every
# part write, so a change to it takes effect without a `<compression>` configuration reload.
node_pco = cluster.add_instance(
    "node_pco",
    main_configs=["configs/pco_compression_selector.xml"],
)
node_zstd = cluster.add_instance(
    "node_zstd",
    main_configs=["configs/zstd_compression_selector.xml"],
)
node_zxc = cluster.add_instance(
    "node_zxc",
    main_configs=["configs/zxc_compression_selector.xml"],
)
node_zxc_allowed = cluster.add_instance(
    "node_zxc_allowed",
    main_configs=["configs/zxc_compression_selector.xml"],
    user_configs=["configs/enable_zxc_codec.xml"],
)
# The global context applies `system_profile`, which deliberately disagrees with the
# default profile here. The selector must follow the latter because it is a durable
# server-level configuration policy.
node_zxc_default_profile_allowed = cluster.add_instance(
    "node_zxc_default_profile_allowed",
    main_configs=["configs/zxc_compression_selector.xml", "configs/system_profile.xml"],
    user_configs=["configs/default_profile_allowed_system_profile_disabled.xml"],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_pco_in_compression_selector_is_rejected(start_cluster):
    node_pco.query(
        "CREATE TABLE t_pco_selector (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )

    # The <compression> selector is built lazily on the first part write, so the rejection surfaces
    # on the first INSERT rather than making the server fail to start.
    with pytest.raises(QueryRuntimeException) as exc:
        node_pco.query("INSERT INTO t_pco_selector SELECT number FROM numbers(1000)")

    message = str(exc.value)
    assert "requires a column type" in message, message
    assert "PCO" in message, message

    node_pco.query("DROP TABLE t_pco_selector")


def test_experimental_codec_in_compression_selector_is_rejected(start_cluster):
    node_zxc.query(
        "CREATE TABLE t_zxc_selector (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )

    with pytest.raises(QueryRuntimeException) as exc:
        node_zxc.query("INSERT INTO t_zxc_selector SELECT number FROM numbers(1000)")

    message = str(exc.value)
    assert "enable_zxc_codec" in message, message
    assert "ZXC" in message, message

    # The query-level setting does not open the server-level configuration.
    with pytest.raises(QueryRuntimeException) as exc:
        node_zxc.query(
            "INSERT INTO t_zxc_selector SELECT number FROM numbers(1000)",
            settings={"enable_zxc_codec": 1},
        )
    assert "enable_zxc_codec" in str(exc.value), str(exc.value)

    node_zxc.query("DROP TABLE t_zxc_selector")


def test_experimental_codec_in_compression_selector_is_allowed_by_the_policy(
    start_cluster,
):
    node_zxc_allowed.query(
        "CREATE TABLE t_zxc_allowed (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_zxc_allowed.query(
        "INSERT INTO t_zxc_allowed SELECT number FROM numbers(1000)"
    )
    assert node_zxc_allowed.query("SELECT count() FROM t_zxc_allowed").strip() == "1000"
    default_codec = node_zxc_allowed.query(
        "SELECT default_compression_codec FROM system.parts"
        " WHERE table = 't_zxc_allowed' AND active"
    ).strip()
    assert "ZXC" in default_codec, default_codec
    node_zxc_allowed.query("DROP TABLE t_zxc_allowed")


def test_compression_selector_uses_default_not_system_profile(start_cluster):
    node_zxc_default_profile_allowed.query(
        "CREATE TABLE t_zxc_default_profile (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_zxc_default_profile_allowed.query(
        "INSERT INTO t_zxc_default_profile SELECT number FROM numbers(1000)"
    )
    assert (
        node_zxc_default_profile_allowed.query(
            "SELECT count() FROM t_zxc_default_profile"
        )
        == "1000\n"
    )
    node_zxc_default_profile_allowed.query("DROP TABLE t_zxc_default_profile")


ENABLE_ZXC_CODEC_POLICY = """
<clickhouse>
    <profiles>
        <default>
            <enable_zxc_codec>{value}</enable_zxc_codec>
        </default>
    </profiles>
</clickhouse>
"""


def test_compression_selector_follows_default_profile_policy(start_cluster):
    # The selector follows the policy in force at each part write, in both directions and without
    # the `<compression>` configuration itself ever being reloaded. A selector cached across a
    # policy change would keep writing parts with a codec the policy no longer allows - and, once
    # the policy allows it again, keep refusing it.
    node_zxc_allowed.query(
        "CREATE TABLE t_zxc_reload_policy (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_zxc_allowed.query(
        "INSERT INTO t_zxc_reload_policy SELECT number FROM numbers(1000)"
    )

    node_zxc_allowed.replace_config(
        "/etc/clickhouse-server/users.d/enable_zxc_codec.xml",
        ENABLE_ZXC_CODEC_POLICY.format(value=0),
    )
    node_zxc_allowed.query("SYSTEM RELOAD USERS")

    with pytest.raises(QueryRuntimeException) as exc:
        node_zxc_allowed.query(
            "INSERT INTO t_zxc_reload_policy SELECT number FROM numbers(1000)"
        )
    assert "enable_zxc_codec" in str(exc.value), str(exc.value)

    node_zxc_allowed.replace_config(
        "/etc/clickhouse-server/users.d/enable_zxc_codec.xml",
        ENABLE_ZXC_CODEC_POLICY.format(value=1),
    )
    node_zxc_allowed.query("SYSTEM RELOAD USERS")

    node_zxc_allowed.query(
        "INSERT INTO t_zxc_reload_policy SELECT number FROM numbers(1000)"
    )
    default_codecs = node_zxc_allowed.query(
        "SELECT DISTINCT default_compression_codec FROM system.parts"
        " WHERE table = 't_zxc_reload_policy' AND active"
    ).strip()
    assert "ZXC" in default_codecs, default_codecs

    node_zxc_allowed.query("DROP TABLE t_zxc_reload_policy")


def test_normal_compression_selector_still_works(start_cluster):
    node_zstd.query(
        "CREATE TABLE t_zstd_selector (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_zstd.query("INSERT INTO t_zstd_selector SELECT number FROM numbers(1000)")
    assert (
        node_zstd.query("SELECT count() FROM t_zstd_selector").strip() == "1000"
    )
    node_zstd.query("DROP TABLE t_zstd_selector")
