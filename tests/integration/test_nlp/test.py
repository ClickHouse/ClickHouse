import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance('instance', main_configs=['configs/config.xml'])

def copy_file_to_container(local_path, dist_path, container_id):
    os.system("docker cp {local} {cont_id}:{dist}".format(local=local_path, cont_id=container_id, dist=dist_path))

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        copy_file_to_container(os.path.join(SCRIPT_DIR, 'dictionaries/.'), '/etc/clickhouse-server/dictionaries', instance.docker_id)

        yield cluster
    finally:
        cluster.shutdown()

def test_lemmatize(start_cluster):
    assert instance.query("SELECT lemmatize('en', 'wolves')") == "wolf"
    assert instance.query("SELECT lemmatize('en', 'dogs')") == "dog"
    assert instance.query("SELECT lemmatize('en', 'looking')") == "look"
    assert instance.query("SELECT lemmatize('en', 'took')") == "take"
    assert instance.query("SELECT lemmatize('en', 'imported')") == "import"
    assert instance.query("SELECT lemmatize('en', 'tokenized')") == "tokenize"
    assert instance.query("SELECT lemmatize('en', 'flown')") == "fly"

def test_synonyms_extensions(start_cluster):
    assert instance.query("SELECT synonyms('en', 'crucial')") == "['important','big','critical','crucial','essential']"
    assert instance.query("SELECT synonyms('en', 'cheerful')") == "['happy','cheerful','delighted','ecstatic']"
    assert instance.query("SELECT synonyms('en', 'yet')") == "['however','nonetheless','but','yet']"
    assert instance.query("SELECT synonyms('en', 'quiz')") == "['quiz','query','check','exam']"
    
    assert instance.query("SELECT synonyms('ru', 'главный')") == "['важный','большой','высокий','хороший','главный']"
    assert instance.query("SELECT synonyms('ru', 'веселый')") == "['веселый','счастливый','живой','яркий','смешной]"
    assert instance.query("SELECT synonyms('ru', 'правда')") == "['хотя','однако','но','правда']"
    assert instance.query("SELECT synonyms('ru', 'экзамен')") == "['экзамен','испытание','проверка']"

