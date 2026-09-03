import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", user_configs=["configs/combined_profile.xml"]
)
q = instance.query


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_combined_profile(started_cluster):
    settings = q(
        """
SELECT name, value FROM system.settings
    WHERE name IN
      ('max_insert_block_size', 'max_network_bytes', 'max_query_size',
       'max_parallel_replicas', 'readonly')
    AND changed
ORDER BY name
""",
        user="test_combined_profile",
    )

    expected1 = """\
max_insert_block_size	654321
max_network_bytes	1234567890
max_parallel_replicas	2
max_query_size	400000000
readonly	2"""

    assert TSV(settings) == TSV(expected1)

    with pytest.raises(QueryRuntimeException) as exc:
        q(
            """
          SET max_insert_block_size = 1000;
          """,
            user="test_combined_profile",
        )

    assert "max_insert_block_size shouldn't be less than 654320." in str(exc.value)

    with pytest.raises(QueryRuntimeException) as exc:
        q(
            """
          SET max_network_bytes = 2000000000;
          """,
            user="test_combined_profile",
        )

    assert "max_network_bytes shouldn't be greater than 1234567891." in str(exc.value)

    with pytest.raises(QueryRuntimeException) as exc:
        q(
            """
          SET max_parallel_replicas = 1000;
          """,
            user="test_combined_profile",
        )

    assert "max_parallel_replicas should not be changed." in str(exc.value)

    with pytest.raises(QueryRuntimeException) as exc:
        q(
            """
          SET max_memory_usage = 1000;
          """,
            user="test_combined_profile",
        )

    assert "max_memory_usage shouldn't be less than 300000000." in str(exc.value)
