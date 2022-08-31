import pytest

from helpers.cluster import ClickHouseCluster

DICTIONARIES = ["configs/dictionaries/regexp_tree_dict.xml"]
CONFIG_FILES = ["configs/regexp_tree_config.yaml"]

DICT_NAME = 'ext-dict-yaml'

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance", main_configs=CONFIG_FILES, dictionaries=DICTIONARIES
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_simple_get(started_cluster):
    query = instance.query

    result = query(
        f"""
        SELECT dictGet('{DICT_NAME}', 'name', 'InspectBrowser');
        """
    )

    assert result.strip() == 'Inspect Browser'


def test_nested_get(started_cluster):
    query = instance.query

    result = query(
        f"""
        SELECT dictGet('{DICT_NAME}', ('name', 'version', 'code'), 'Google Chrome Chromium 1994');
        """
    )

    assert result.strip() == "('Chromium 1994','1994 amd64','Google Chrome Chromium 1994')"


def test_collecting_get(started_cluster):
    query = instance.query

    result = query(
        f"""
        SELECT dictGet('{DICT_NAME}', ('name', 'version', 'code'), 'Mozilla/5.0');
        """
    )

    assert result.strip() == "('Mozilla Correct','5.0','5.0')"


def test_not_found(started_cluster):
    query = instance.query

    result = query(
        f"""
        SELECT dictGet('{DICT_NAME}', 'name', 'Some weird key');
        """
    )

    assert result.strip() == '\\N'


def test_not_all_attributes_found(started_cluster):
    query = instance.query

    result = query(
        f"""
        SELECT dictGet('{DICT_NAME}', ('name', 'version'), 'InspectBrowser');
        """
    )

    assert result.strip() == "('Inspect Browser',NULL)"
