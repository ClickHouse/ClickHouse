import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The server-level <compression> selector chooses the default codec of every part. That codec is
# fed raw (without a column type) into the statistics and text-index streams, so it must not be an
# experimental codec (which would bypass the per-column allow_experimental_codecs gate) nor a codec
# that requires a column type (e.g. PCO). Both must be rejected when the configuration is loaded.
node_pco = cluster.add_instance(
    "node_pco",
    main_configs=["configs/pco_compression_selector.xml"],
)
node_zstd = cluster.add_instance(
    "node_zstd",
    main_configs=["configs/zstd_compression_selector.xml"],
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
    assert "experimental codec PCO" in message, message

    node_pco.query("DROP TABLE t_pco_selector")


def test_normal_compression_selector_still_works(start_cluster):
    node_zstd.query(
        "CREATE TABLE t_zstd_selector (x UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )
    node_zstd.query("INSERT INTO t_zstd_selector SELECT number FROM numbers(1000)")
    assert (
        node_zstd.query("SELECT count() FROM t_zstd_selector").strip() == "1000"
    )
    node_zstd.query("DROP TABLE t_zstd_selector")
