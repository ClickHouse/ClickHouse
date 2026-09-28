import pytest

import helpers
from helpers.cluster import ClickHouseCluster


def test_yaml_merge_keys_conf():
    cluster = ClickHouseCluster(__file__)
    node = cluster.add_instance("node", user_configs=["configs/merge_keys.yml"])

    try:
        cluster.start()

        # Assert simple merge key substitution
        assert node.query("select getSetting('max_threads')", user="user_1") == "1\n"

        # Assert merge key overriden by regular key
        assert node.query("select getSetting('max_threads')", user="user_2") == "4\n"

        # Assert normal key overriden by merge key
        assert node.query("select getSetting('max_threads')", user="user_3") == "4\n"

        # Assert override with multiple merge keys
        assert (
            node.query("select getSetting('max_final_threads')", user="user_4") == "2\n"
        )

        # Assert multiple merge key substitutions overriden by regular key
        assert node.query("select getSetting('max_threads')", user="user_4") == "4\n"

        # Assert override with multiple merge keys for list syntax
        assert (
            node.query("select getSetting('max_final_threads')", user="user_5") == "2\n"
        )

        # Assert multiple merge key substitutions overriden by regular key
        # for list syntax
        assert node.query("select getSetting('max_threads')", user="user_5") == "4\n"
    finally:
        cluster.shutdown()
