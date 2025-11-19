import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

DICTIONARY_FILES = [
    "dictionaries/long_loading_dictionary.xml",
]

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/no_dictionaries_lazy_load.xml"],
    dictionaries=DICTIONARY_FILES,
)

node0 = cluster.add_instance(
    "node0",
    dictionaries=DICTIONARY_FILES,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_status(instance, dictionary_name):
    return instance.query(
        "SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'"
    ).rstrip("\n")


def test_wait_for_dictionaries_load():
    assert get_status(node1, "long_loading_dictionary") == "LOADED"
    assert node1.query("SELECT * FROM dictionary(long_loading_dictionary)") == TSV(
        [[1, "aa"], [2, "bb"]]
    )

    assert get_status(node0, "long_loading_dictionary") == "NOT_LOADED"
    assert node0.query("SELECT * FROM dictionary(long_loading_dictionary)") == TSV(
        [[1, "aa"], [2, "bb"]]
    )
    assert get_status(node0, "long_loading_dictionary") == "LOADED"
