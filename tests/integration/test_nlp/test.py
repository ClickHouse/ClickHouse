import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', base_config_dir='configs/')

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
    assert instance.query("SELECT lemmatize('en', 'wolves')") == "wolf\n"
    assert instance.query("SELECT lemmatize('en', 'dogs')") == "dog\n"
    assert instance.query("SELECT lemmatize('en', 'looking')") == "look\n"
    assert instance.query("SELECT lemmatize('en', 'took')") == "take\n"
    assert instance.query("SELECT lemmatize('en', 'imported')") == "import\n"
    assert instance.query("SELECT lemmatize('en', 'tokenized')") == "tokenize\n"
    assert instance.query("SELECT lemmatize('en', 'flown')") == "fly\n"

def test_synonyms_extensions(start_cluster):
    assert instance.query("SELECT synonyms('en', 'crucial')") == "['important','big','critical','crucial','essential']\n"
    assert instance.query("SELECT synonyms('en', 'cheerful')") == "['happy','cheerful','delighted','ecstatic']\n"
    assert instance.query("SELECT synonyms('en', 'yet')") == "['however','nonetheless','but','yet']\n"
    assert instance.query("SELECT synonyms('en', 'quiz')") == "['quiz','query','check','exam']\n"
    
    assert instance.query("SELECT synonyms('ru', 'главный')") == "['важный','большой','высокий','хороший','главный']\n"
    assert instance.query("SELECT synonyms('ru', 'веселый')") == "['веселый','счастливый','живой','яркий','смешной']\n"
    assert instance.query("SELECT synonyms('ru', 'правда')") == "['хотя','однако','но','правда']\n"
    assert instance.query("SELECT synonyms('ru', 'экзамен')") == "['экзамен','испытание','проверка']\n"

