import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

DICTIONARY_FILES = [
    "configs/dictionaries/invalid_dict.xml",
    "configs/dictionaries/valid_dict.xml",
]

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", dictionaries=DICTIONARY_FILES)

VALID_DICT_NAME = "valid_dict"
INVALID_DICT_NAME = "invalid_dict"
UNKNOWN_DATA_TYPE_EXCEPTION_STR = "DB::Exception: Unknown data type"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_select_from_system_dictionaries_with_invalid_dictionary(started_cluster):
    query = instance.query

    assert query("SELECT name FROM system.dictionaries;").splitlines() == [
        VALID_DICT_NAME,
        INVALID_DICT_NAME,
    ]

    assert (
        query(
            f"select last_exception from system.dictionaries WHERE name='{VALID_DICT_NAME}';"
        ).strip()
        == ""
    )

    assert (
        UNKNOWN_DATA_TYPE_EXCEPTION_STR
        in query(
            f"select last_exception from system.dictionaries WHERE name='{INVALID_DICT_NAME}';"
        ).strip()
    )


def test_dictGet_func_for_invalid_dictionary(started_cluster):
    query = instance.query

    with pytest.raises(QueryRuntimeException) as exc:
        query(
            f"SELECT dictGetString('{INVALID_DICT_NAME}', 'invalid_attr', toInt64(1));"
        )
    assert UNKNOWN_DATA_TYPE_EXCEPTION_STR in str(exc.value)
