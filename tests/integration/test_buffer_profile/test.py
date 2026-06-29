# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_default = cluster.add_instance("node_default")
node_buffer_profile = cluster.add_instance(
    "node_buffer_profile",
    main_configs=["configs/buffer_profile.xml"],
    user_configs=["configs/users.d/buffer_profile.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def bootstrap(node):
    node.query(
        """
    CREATE TABLE data (key Int) Engine=MergeTree()
    ORDER BY key
    PARTITION BY key % 2;

    CREATE TABLE buffer AS data Engine=Buffer(currentDatabase(), data,
            /* settings for manual flush only */
            1,    /* num_layers */
            10e6, /* min_time, placeholder */
            10e6, /* max_time, placeholder */
            0,    /* min_rows   */
            10e6, /* max_rows   */
            0,    /* min_bytes  */
            80e6  /* max_bytes  */
    );

    INSERT INTO buffer SELECT * FROM numbers(100);
    """
    )


def test_default_profile():
    bootstrap(node_default)
    # flush the buffer
    node_default.query("OPTIMIZE TABLE buffer")


def test_buffer_profile():
    bootstrap(node_buffer_profile)
    with pytest.raises(
        QueryRuntimeException, match="Too many partitions for single INSERT block"
    ):
        # flush the buffer
        node_buffer_profile.query("OPTIMIZE TABLE buffer")
