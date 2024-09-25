import os
import math
import pytest

from .common import *

from helpers.cluster import ClickHouseCluster
from helpers.dictionary import Field, Row, Dictionary, DictionaryStructure, Layout
from helpers.external_sources import SourceMongo

simple_tester = None
complex_tester = None
ranged_tester = None
TEST_NAME = "mongo"

cluster_legacy = ClickHouseCluster(f"{__file__}_legacy")
cluster_new = ClickHouseCluster(f"{__file__}_new")


@pytest.fixture(scope="module")
def secure_connection(request):
    return request.param


@pytest.fixture(scope="module")
def legacy(request):
    return request.param


@pytest.fixture(scope="module")
def source(secure_connection, legacy, started_cluster):
    print(f"XXXXX: {started_cluster.mongo_secure_port} {started_cluster.mongo_port} {secure_connection} {legacy}")
    return SourceMongo(
        f"MongoDB",
        "localhost",
        started_cluster.mongo_secure_port if secure_connection else started_cluster.mongo_port,
        "mongo_secure" if secure_connection else "mongo1",
        27017,
        "root",
        "clickhouse",
        secure=secure_connection,
        legacy=legacy,
    )


@pytest.fixture(scope="module")
def simple_tester(source, started_cluster):
    tester = SimpleLayoutTester(TEST_NAME)
    tester.cleanup()
    tester.create_dictionaries(source)
    tester.prepare(started_cluster)
    return tester


@pytest.fixture(scope="module")
def complex_tester(source, started_cluster):
    tester = ComplexLayoutTester(TEST_NAME)
    tester.create_dictionaries(source)
    tester.prepare(started_cluster)
    return tester


@pytest.fixture(scope="module")
def ranged_tester(source, started_cluster):
    tester = RangedLayoutTester(TEST_NAME)
    tester.create_dictionaries(source)
    tester.prepare(started_cluster)
    return tester


def get_config(secure_connection, legacy):
    if legacy:
        main_config = [os.path.join("configs", "mongo", "legacy.xml")]
    else:
        main_config = [os.path.join("configs", "mongo", "new.xml")]

    if secure_connection:
        main_config.append(os.path.join("configs", "disable_ssl_verification.xml"))
    else:
        main_config.append(os.path.join("configs", "ssl_verification.xml"))

    return main_config


def node_name(secure_connection, legacy):
    secure_connection_suffix = "secure" if secure_connection else "non_secure"
    legacy_suffix = "legacy" if legacy else "new"
    return f"node_mongo_{secure_connection_suffix}_{legacy_suffix}"


def setup_nodes(is_secure, is_legacy):
    cluster = cluster_legacy if is_legacy else cluster_new
    return cluster.add_instance(
        node_name(is_secure, is_legacy),
        main_configs=get_config(is_secure, is_legacy),
        dictionaries=BaseLayoutTester.list_dictionaries(TEST_NAME),
        with_mongo=True,
    )

all_nodes = [
    setup_nodes(is_secure, is_legacy)
    for is_secure in [False, True]
    for is_legacy in [False, True]
]


# mongo_sources = [
#     SourceMongo(
#         "MongoDB",
#         "localhost",
#         27017,
#         "mongo_secure" if is_secure else "mongo1",
#         27017,
#         "root",
#         "clickhouse",
#         secure=is_secure,
#         legacy=is_legacy,
#     )
#     for is_secure in [False, True]
#     for is_legacy in [False, True]
# ]


@pytest.fixture(scope="module")
def started_cluster(legacy):
    cluster = cluster_legacy if legacy else cluster_new
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple(secure_connection, legacy, started_cluster, layout_name, simple_tester):
    simple_tester.execute(
        layout_name, started_cluster.instances[node_name(secure_connection, legacy)]
    )


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_COMPLEX))
def test_complex(
    secure_connection, legacy, started_cluster, layout_name, complex_tester
):
    complex_tester.execute(
        layout_name, started_cluster.instances[node_name(secure_connection, legacy)]
    )


@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_RANGED))
def test_ranged(secure_connection, legacy, started_cluster, layout_name, ranged_tester):
    ranged_tester.execute(
        layout_name, started_cluster.instances[node_name(secure_connection, legacy)]
    )


@pytest.mark.parametrize("secure_connection", [True], indirect=["secure_connection"])
@pytest.mark.parametrize("legacy", [False, True], indirect=["legacy"])
@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_SIMPLE))
def test_simple_ssl(
    secure_connection, legacy, started_cluster, layout_name, simple_tester
):
    simple_tester.execute(
        layout_name, started_cluster.instances[node_name(secure_connection, legacy)]
    )
