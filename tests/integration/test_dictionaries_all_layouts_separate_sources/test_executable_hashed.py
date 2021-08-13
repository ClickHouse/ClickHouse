import os 
import math
import pytest

from .common import *

from helpers.cluster import ClickHouseCluster
from helpers.dictionary import Field, Row, Dictionary, DictionaryStructure, Layout
from helpers.external_sources import SourceExecutableHashed

SOURCE = SourceExecutableHashed("ExecutableHashed", "localhost", "9000", "node", "9000", "", "")

cluster = None
node = None
simple_tester = None
complex_tester = None
ranged_tester = None


def setup_module(module):
    global cluster
    global node
    global simple_tester
    global complex_tester
    global ranged_tester

    for f in os.listdir(DICT_CONFIG_PATH):
        os.remove(os.path.join(DICT_CONFIG_PATH, f))

    simple_tester = SimpleLayoutTester()
    simple_tester.create_dictionaries(SOURCE)

    complex_tester = ComplexLayoutTester()
    complex_tester.create_dictionaries(SOURCE)

    ranged_tester = RangedLayoutTester()
    ranged_tester.create_dictionaries(SOURCE)
    # Since that all .xml configs were created

    cluster = ClickHouseCluster(__file__)

    dictionaries = []
    main_configs = []
    main_configs.append(os.path.join('configs', 'disable_ssl_verification.xml'))
    
    for fname in os.listdir(DICT_CONFIG_PATH):
        dictionaries.append(os.path.join(DICT_CONFIG_PATH, fname))

    node = cluster.add_instance('node', main_configs=main_configs, dictionaries=dictionaries)

    
def teardown_module(module):
    global DICT_CONFIG_PATH
    for fname in os.listdir(DICT_CONFIG_PATH):
        os.remove(os.path.join(DICT_CONFIG_PATH, fname))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        simple_tester.prepare(cluster)
        complex_tester.prepare(cluster)
        ranged_tester.prepare(cluster)

        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize("layout_name", ['hashed'])
def test_simple(started_cluster, layout_name):
    simple_tester.execute(layout_name, node)

@pytest.mark.parametrize("layout_name", ['complex_key_hashed'])
def test_complex(started_cluster, layout_name):
    complex_tester.execute(layout_name, node)

@pytest.mark.parametrize("layout_name", sorted(LAYOUTS_RANGED))
def test_ranged(started_cluster, layout_name):
    ranged_tester.execute(layout_name, node)
