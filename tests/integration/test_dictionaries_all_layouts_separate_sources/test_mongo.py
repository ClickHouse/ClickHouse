import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import mongo_pass
from helpers.external_sources import SourceMongo

from .common import *

simple_tester = None
complex_tester = None
ranged_tester = None
test_name = "mongo"


@pytest.fixture(scope="module")
def secure_connection(request):
    return request.param


@pytest.fixture(scope="module")
def cluster(secure_connection):
    cluster_name = __file__.removeprefix("test_").removesuffix(".py")
    cluster_name += "_secure" if secure_connection else "_insecure"
    return ClickHouseCluster(cluster_name)


@pytest.fixture(scope="module")
def source(secure_connection, cluster):
    return SourceMongo(
        "MongoDB",
        "localhost",
        cluster.mongo_secure_port if secure_connection else cluster.mongo_port,
        "mongo_secure" if secure_connection else "mongo1",
        27017,
        "root",
        mongo_pass,
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
    if secure_connection:
        return [os.path.join("configs", "disable_ssl_verification.xml")]
    return [os.path.join("configs", "ssl_verification.xml")]


@pytest.fixture(scope="module")
def started_cluster(
    cluster,
    main_config,
    simple_tester,
    ranged_tester,
    complex_tester,
):
    dictionaries = simple_tester.list_dictionaries()
    cluster.add_instance(
        "node1",
        main_configs=main_config,
        dictionaries=dictionaries,
        with_mongo=True,
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
    simple_tester.execute(layout_name, started_cluster.instances["node1"])


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_COMPLEX))
def test_complex(secure_connection, started_cluster, layout_name, complex_tester):
    complex_tester.execute(layout_name, started_cluster.instances["node1"])


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_RANGED))
def test_ranged(secure_connection, started_cluster, layout_name, ranged_tester):
    ranged_tester.execute(layout_name, started_cluster.instances["node1"])


@pytest.mark.parametrize("secure_connection", [True], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple_ssl(secure_connection, started_cluster, layout_name, simple_tester):
    simple_tester.execute(layout_name, started_cluster.instances["node1"])
