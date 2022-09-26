import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", main_configs=["configs/dicts_config.xml"])


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "dictionaries/."),
            "/etc/clickhouse-server/dictionaries",
            instance.docker_id,
        )

        yield cluster
    finally:
        cluster.shutdown()


def test_lemmatize(start_cluster):
    assert (
        instance.query(
            "SELECT lemmatize('en', 'wolves')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "wolf\n"
    )
    assert (
        instance.query(
            "SELECT lemmatize('en', 'dogs')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "dog\n"
    )
    assert (
        instance.query(
            "SELECT lemmatize('en', 'looking')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "look\n"
    )
    assert (
        instance.query(
            "SELECT lemmatize('en', 'took')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "take\n"
    )
    assert (
        instance.query(
            "SELECT lemmatize('en', 'imported')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "import\n"
    )
    assert (
        instance.query(
            "SELECT lemmatize('en', 'tokenized')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "tokenize\n"
    )
    assert (
        instance.query(
            "SELECT lemmatize('en', 'flown')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "fly\n"
    )


def test_synonyms_extensions(start_cluster):
    assert (
        instance.query(
            "SELECT synonyms('en', 'crucial')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['important','big','critical','crucial','essential']\n"
    )
    assert (
        instance.query(
            "SELECT synonyms('en', 'cheerful')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['happy','cheerful','delighted','ecstatic']\n"
    )
    assert (
        instance.query(
            "SELECT synonyms('en', 'yet')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['however','nonetheless','but','yet']\n"
    )
    assert (
        instance.query(
            "SELECT synonyms('en', 'quiz')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['quiz','query','check','exam']\n"
    )

    assert (
        instance.query(
            "SELECT synonyms('ru', 'главный')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['важный','большой','высокий','хороший','главный']\n"
    )
    assert (
        instance.query(
            "SELECT synonyms('ru', 'веселый')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['веселый','счастливый','живой','яркий','смешной']\n"
    )
    assert (
        instance.query(
            "SELECT synonyms('ru', 'правда')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['хотя','однако','но','правда']\n"
    )
    assert (
        instance.query(
            "SELECT synonyms('ru', 'экзамен')",
            settings={"allow_experimental_nlp_functions": 1},
        )
        == "['экзамен','испытание','проверка']\n"
    )
