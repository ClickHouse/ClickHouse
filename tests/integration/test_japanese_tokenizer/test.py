import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node", main_configs=["configs/mecab_tokenizer.xml"], stay_alive=True
)
node_bad_sha = cluster.add_instance(
    "node_bad_sha", main_configs=["configs/mecab_tokenizer_bad_sha.xml"], stay_alive=True
)
node_s3 = cluster.add_instance(
    "node_s3", main_configs=["configs/mecab_tokenizer_s3.xml"], with_minio=True, stay_alive=True
)

DICT_FILE = "minimal_dic.tar.gz"
DICT_IN_CONTAINER = "/var/lib/clickhouse/user_files/" + DICT_FILE


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        dict_path = os.path.join(SCRIPT_DIR, "dictionary", DICT_FILE)
        for instance in (node, node_bad_sha):
            instance.copy_file_to_container(dict_path, DICT_IN_CONTAINER)
        # Upload the same fixture to MinIO for the s3:// test.
        cluster.minio_client.fput_object(cluster.minio_bucket, DICT_FILE, dict_path)
        yield cluster
    finally:
        cluster.shutdown()


def skip_if_no_mecab(instance):
    if instance.query("SELECT count() FROM system.tokenizers WHERE name = 'japanese'").strip() != "1":
        pytest.skip("MeCab is not enabled in this build")


def test_tokens_function(started_cluster):
    skip_if_no_mecab(node)
    assert (
        node.query("SELECT tokens('日本語の形態素解析エンジン', 'japanese')").strip()
        == "['日本語','の','形態','素','解析','エンジン']"
    )
    assert (
        node.query("SELECT tokens('これはテストの文章です', 'japanese')").strip()
        == "['これ','は','テスト','の','文章','です']"
    )


def test_text_index(started_cluster):
    skip_if_no_mecab(node)
    node.query("DROP TABLE IF EXISTS jp")
    node.query(
        """
        CREATE TABLE jp (id UInt32, s String,
            INDEX idx s TYPE text(tokenizer = 'japanese') GRANULARITY 1)
        ENGINE = MergeTree ORDER BY id
        """
    )
    node.query(
        "INSERT INTO jp VALUES (1, '日本語の形態素解析エンジン'), (2, 'これはテストの文章です')"
    )

    assert node.query("SELECT id FROM jp WHERE hasAllTokens(s, '形態', 'japanese')").strip() == "1"
    assert node.query("SELECT id FROM jp WHERE hasAnyTokens(s, 'テスト', 'japanese')").strip() == "2"
    assert node.query("SELECT id FROM jp WHERE hasAllTokens(s, '形態 解析', 'japanese')").strip() == "1"
    assert node.query("SELECT count() FROM jp WHERE hasAllTokens(s, '解析', 'japanese')").strip() == "1"

    node.query("DROP TABLE jp")


def test_wrong_sha_fails_closed(started_cluster):
    skip_if_no_mecab(node_bad_sha)
    error = node_bad_sha.query_and_get_error("SELECT tokens('日本語', 'japanese')")
    assert "CHECKSUM_DOESNT_MATCH" in error


def test_dictionary_from_s3(started_cluster):
    # Dictionary fetched from MinIO via the S3 client.
    skip_if_no_mecab(node_s3)
    assert (
        node_s3.query("SELECT tokens('日本語の形態素解析エンジン', 'japanese')").strip()
        == "['日本語','の','形態','素','解析','エンジン']"
    )
