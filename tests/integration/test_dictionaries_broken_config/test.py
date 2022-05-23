import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module", params=["broken_metadata", "missing_executable"])
def started_cluster_instance(request):
    cluster = ClickHouseCluster(__file__)
    try:
        instance = cluster.add_instance(
            request.param,
            main_configs=["configs/config.xml"],
            dictionaries=[
                "configs/dictionaries/good_dict.xml",
                "configs/dictionaries/{}.xml".format(request.param),
            ],
            stay_alive=True,
        )
        cluster.start()

        # Load some data so that the restart is not "empty"
        instance.query(
            """
            CREATE DATABASE IF NOT EXISTS dict ENGINE=Dictionary;
            CREATE DATABASE IF NOT EXISTS test;
            DROP TABLE IF EXISTS test.elements;
            CREATE TABLE test.elements (id UInt64, a String, b Int32, c Float64) ENGINE=Log;
            INSERT INTO test.elements VALUES (0, 'water', 10, 1), (1, 'air', 40, 0.01), (2, 'earth', 100, 1.7);
            """
        )
        yield (request.param, instance)

    finally:
        cluster.shutdown()


def get_status(instance, dictionary_name):
    return instance.query(
        "SELECT status FROM system.dictionaries WHERE name='" + dictionary_name + "'"
    ).rstrip("\n")


def get_exception(instance, dictionary_name):
    return instance.query(
        "SELECT last_exception FROM system.dictionaries WHERE name='"
        + dictionary_name
        + "'"
    ).rstrip("\n")


def test_broken_dict(started_cluster_instance):
    error_type, instance = started_cluster_instance
    instance.restart_clickhouse()
    assert get_status(instance, "good_dict") == "LOADED"
    assert instance.query("select dictGet('good_dict', 'first', toUInt64(1))") == "2\n"

    assert get_status(instance, error_type) == "FAILED"
    assert "DB::Exception" in get_exception(instance, error_type)
    assert "DB::Exception" in instance.query_and_get_error(
        "select dictGet('{}', 'first', toUInt32(1))".format(error_type)
    )
