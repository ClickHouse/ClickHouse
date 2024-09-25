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
def legacy(request):
    return request.param


@pytest.fixture(scope="module")
def cluster(secure_connection):
    return ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def source(secure_connection, legacy, cluster):
    return SourceMongo(
        "MongoDB",
        "localhost",
        cluster.mongo_secure_port if secure_connection else cluster.mongo_port,
        "mongo_secure" if secure_connection else "mongo1",
        27017,
        "root",
        "clickhouse",
        secure=secure_connection,
        legacy=legacy,
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
def main_config(secure_connection, legacy):
    if legacy:
        main_config = [os.path.join("configs", "mongo", "legacy.xml")]
    else:
        main_config = [os.path.join("configs", "mongo", "new.xml")]

    if secure_connection:
        main_config.append(os.path.join("configs", "disable_ssl_verification.xml"))
    else:
        main_config.append(os.path.join("configs", "ssl_verification.xml"))

    return main_config


def get_node_name(secure_connection, legacy):
    secure_connection_suffix = "_secure" if secure_connection else "non_secure"
    legacy_suffix = "_legacy" if legacy else "_new"
    return f"node_mongo_{secure_connection_suffix}_{legacy_suffix}"


@pytest.fixture(scope="module")
def started_cluster(
    secure_connection,
    legacy,
    cluster,
    main_config,
    simple_tester,
    ranged_tester,
    complex_tester,
):
    SOURCE = SourceMongo(
        "MongoDB",
        "localhost",
        27017,
        "mongo_secure" if secure_connection else "mongo1",
        27017,
        "root",
        "clickhouse",
        secure=secure_connection,
        legacy=legacy,
    )
    dictionaries = simple_tester.list_dictionaries()

    node = cluster.add_instance(
        get_node_name(secure_connection, legacy),
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
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple(secure_connection, legacy, started_cluster, layout_name, simple_tester):
    simple_tester.execute(
        layout_name, started_cluster.instances[get_node_name(secure_connection, legacy)]
    )


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_COMPLEX))
def test_complex(
    secure_connection, legacy, started_cluster, layout_name, complex_tester
):
    complex_tester.execute(
        layout_name, started_cluster.instances[get_node_name(secure_connection, legacy)]
    )


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_RANGED))
def test_ranged(secure_connection, legacy, started_cluster, layout_name, ranged_tester):
    ranged_tester.execute(
        layout_name, started_cluster.instances[get_node_name(secure_connection, legacy)]
    )


@pytest.mark.parametrize("secure_connection", [True], indirect=["secure_connection"])
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple_ssl(
    secure_connection, legacy, started_cluster, layout_name, simple_tester
):
    simple_tester.execute(
        layout_name, started_cluster.instances[get_node_name(secure_connection, legacy)]
    )
