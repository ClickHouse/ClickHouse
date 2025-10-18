import math
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.dictionary import Dictionary, DictionaryStructure, Field, Layout, Row
from helpers.external_sources import SourceMongoURI
from helpers.config_cluster import mongo_pass

from .common import *

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
def main_config(secure_connection):
    if secure_connection:
        return [os.path.join("configs", "disable_ssl_verification.xml")]
    return [os.path.join("configs", "ssl_verification.xml")]


@pytest.fixture(scope="module")
def started_cluster(secure_connection, cluster, main_config, simple_tester):
    dictionaries = simple_tester.list_dictionaries()

    node = cluster.add_instance(
        "uri_node",
        main_configs=main_config,
        dictionaries=dictionaries,
        with_mongo=True,
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
