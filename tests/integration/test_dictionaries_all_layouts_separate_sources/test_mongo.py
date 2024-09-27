import os
import math
import pytest

from .common import *

from helpers.cluster import ClickHouseCluster
from helpers.dictionary import Field, Row, Dictionary, DictionaryStructure, Layout
from helpers.external_sources import SourceMongo

SOURCE = None
cluster = None
node = None
simple_tester = None
complex_tester = None
ranged_tester = None
test_name = "mongo"


@pytest.fixture(scope="module")
def secure_connection(request):
    return request.param


@pytest.fixture(scope="module")
def cluster(secure_connection):
    return ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def source(secure_connection, cluster):
    return SourceMongo(
        "MongoDB",
        "localhost",
        cluster.mongo_port,
        cluster.mongo_host,
        "27017",
        "root",
        "clickhouse",
        secure=secure_connection,
    )


@pytest.fixture(scope="module")
def simple_tester(source):
    tester = SimpleLayoutTester(test_name)
    tester.cleanup()
    tester.create_dictionaries(source)
    return tester


@pytest.fixture(scope="module")
def complex_tester(source):
    tester = ComplexLayoutTester(test_name)
    tester.create_dictionaries(source)
    return tester


@pytest.fixture(scope="module")
def ranged_tester(source):
    tester = RangedLayoutTester(test_name)
    tester.create_dictionaries(source)
    return tester


@pytest.fixture(scope="module")
def main_config(secure_connection):
    main_config = []
    if secure_connection:
        main_config.append(os.path.join("configs", "disable_ssl_verification.xml"))
    else:
        main_config.append(os.path.join("configs", "ssl_verification.xml"))
    return main_config


@pytest.fixture(scope="module")
def started_cluster(
    secure_connection,
    cluster,
    main_config,
    simple_tester,
    ranged_tester,
    complex_tester,
):
    SOURCE = SourceMongo(
        "MongoDB",
        "localhost",
        cluster.mongo_port,
        cluster.mongo_host,
        "27017",
        "root",
        "clickhouse",
        secure=secure_connection,
    )
    dictionaries = simple_tester.list_dictionaries()

    node = cluster.add_instance(
        "node",
        main_configs=main_config,
        dictionaries=dictionaries,
        with_mongo=True,
        with_mongo_secure=secure_connection,
    )

    try:
        cluster.start()

        simple_tester.prepare(cluster)
        complex_tester.prepare(cluster)
        ranged_tester.prepare(cluster)

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple(secure_connection, started_cluster, layout_name, simple_tester):
    simple_tester.execute(layout_name, started_cluster.instances["node"])


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_COMPLEX))
def test_complex(secure_connection, started_cluster, layout_name, complex_tester):
    complex_tester.execute(layout_name, started_cluster.instances["node"])


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_RANGED))
def test_ranged(secure_connection, started_cluster, layout_name, ranged_tester):
    ranged_tester.execute(layout_name, started_cluster.instances["node"])


@pytest.mark.parametrize("secure_connection", [True], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple_ssl(secure_connection, started_cluster, layout_name, simple_tester):
    simple_tester.execute(layout_name, started_cluster.instances["node"])
