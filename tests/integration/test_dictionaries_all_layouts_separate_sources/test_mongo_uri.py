import os
import math
import pytest

from .common import *

from helpers.cluster import ClickHouseCluster
from helpers.dictionary import Field, Row, Dictionary, DictionaryStructure, Layout
from helpers.external_sources import SourceMongoURI

test_name = "mongo_uri"


@pytest.fixture(scope="module")
def secure_connection(request):
    return request.param


@pytest.fixture(scope="module")
def cluster(secure_connection):
    return ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def source(secure_connection, cluster):
    return SourceMongoURI(
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
def main_config(secure_connection):
    main_config = []
    if secure_connection:
        main_config.append(os.path.join("configs", "disable_ssl_verification.xml"))
    else:
        main_config.append(os.path.join("configs", "ssl_verification.xml"))
    return main_config


@pytest.fixture(scope="module")
def started_cluster(secure_connection, cluster, main_config, simple_tester):
    dictionaries = simple_tester.list_dictionaries()

    node = cluster.add_instance(
        "uri_node",
        main_configs=main_config,
        dictionaries=dictionaries,
        with_mongo=True,
        with_mongo_secure=secure_connection,
    )
    try:
        cluster.start()
        simple_tester.prepare(cluster)
        yield cluster
    finally:
        cluster.shutdown()


# See comment in SourceMongoURI
@pytest.mark.parametrize("secure_connection", [False], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", ["flat"])
def test_simple(secure_connection, started_cluster, simple_tester, layout_name):
    simple_tester.execute(layout_name, started_cluster.instances["uri_node"])


@pytest.mark.parametrize("secure_connection", [True], indirect=["secure_connection"])
@pytest.mark.parametrize("layout_name", ["flat"])
def test_simple_ssl(secure_connection, started_cluster, simple_tester, layout_name):
    simple_tester.execute(layout_name, started_cluster.instances["uri_node"])
