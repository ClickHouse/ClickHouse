import pytest

from helpers.cluster import ClickHouseCluster

from .yt_helpers import YT_DEFAULT_TOKEN, YT_HOST, YT_PORT, YT_URI, YTsaurusCLI

from helpers.cluster import is_arm


if is_arm():
    # skip due to no arm support for ytsaurus-backend docker image
    # https://github.com/ytsaurus/ytsaurus/blob/main/BUILD.md
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/display_secrets.xml"],
    user_configs=["configs/allow_experimental_ytsaurus.xml"],
    with_ytsaurus=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "dynamic_table, layout",
    [
        (False, "FLAT"),
        (True, "FLAT"),
        (False, "HASHED"),
        (True, "HASHED"),
        (False, "HASHED_ARRAY"),
        (True, "HASHED_ARRAY"),
    ],
)
def test_yt_dictionary_id(started_cluster, dynamic_table, layout):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    path = "//tmp/table"

    yt.create_table(
        path,
        '{"id":1,"value":20}{"id":2,"value":40}{"id":3,"value":30}',
        schema={"id": "int32", "value": "int32"},
        dynamic=dynamic_table,
    )
    instance.query(
        f"CREATE DICTIONARY yt_dict(id UInt64, value Int32) PRIMARY KEY id SOURCE(YTSAURUS(http_proxy_urls '{YT_URI}' cypress_path '{path}' oauth_token '{YT_DEFAULT_TOKEN}' check_table_schema 0)) LAYOUT({layout}()) LIFETIME(MIN 0 MAX 1000)"
    )
    assert (
        instance.query("SELECT dictGet('yt_dict', 'value', number + 1) FROM numbers(3)")
        == "20\n40\n30\n"
    )
    assert instance.query("SELECT dictGet('yt_dict', 'value', 2)") == "40\n"

    instance.query("DROP DICTIONARY yt_dict")
    yt.remove_table(path)


@pytest.mark.parametrize(
    "dynamic_table, layout",
    [
        (True, "COMPLEX_KEY_HASHED"),
        (False, "COMPLEX_KEY_HASHED"),
        (True, "COMPLEX_KEY_HASHED_ARRAY"),
        (False, "COMPLEX_KEY_HASHED_ARRAY"),
    ],
)
def test_yt_dictionary_complex_key(started_cluster, dynamic_table, layout):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    path = "//tmp/table"
    yt.create_table(
        path,
        '{"id1":1, "id2":1, "value":20}{"id1":2, "id2":2, "value":40}{"id1":3, "id2":3, "value":30}',
        schema={"id1": "int32", "id2": "int32", "value": "int32"},
        dynamic=dynamic_table,
    )

    instance.query(
        f"CREATE DICTIONARY yt_dict(id1 Int32, id2 Int32, value Int32) PRIMARY KEY id1, id2 SOURCE(YTSAURUS(http_proxy_urls '{YT_URI}' cypress_path '{path}' oauth_token '{YT_DEFAULT_TOKEN}')) LAYOUT({layout}()) LIFETIME(MIN 0 MAX 1000)"
    )
    assert (
        instance.query(
            "SELECT dictGet('yt_dict', 'value', (number + 1, number + 1)) FROM numbers(3)"
        )
        == "20\n40\n30\n"
    )
    assert instance.query("SELECT dictGet('yt_dict', 'value', (2, 2))") == "40\n"

    instance.query("DROP DICTIONARY yt_dict")
    yt.remove_table(path)


@pytest.mark.parametrize(
    "dynamic_table",
    [
        (True),
        (False),
    ],
)
def test_yt_dictionary_cache_id(started_cluster, dynamic_table):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    path = "//tmp/table"

    yt.create_table(
        path,
        '{"id":1,"value":20}{"id":2,"value":40}{"id":3,"value":30}',
        sorted_columns=("id"),
        schema={"id": "int32", "value": "int32"},
        dynamic=dynamic_table,
    )
    instance.query(
        f"CREATE DICTIONARY yt_dict(id UInt64, value Int32) PRIMARY KEY id SOURCE(YTSAURUS(http_proxy_urls '{YT_URI}' cypress_path '{path}' oauth_token '{YT_DEFAULT_TOKEN}' check_table_schema 0)) LAYOUT(CACHE(SIZE_IN_CELLS 10)) LIFETIME(MIN 0 MAX 1000)"
    )
    if dynamic_table:
        assert (
            instance.query(
                "SELECT dictGet('yt_dict', 'value', number + 1) FROM numbers(3)"
            )
            == "20\n40\n30\n"
        )
        assert instance.query("SELECT dictGet('yt_dict', 'value', 2)") == "40\n"
    else:
        ## Cached dictionaries are not supported with static ytsaurus tables.
        instance.query_and_get_error(
            "SELECT dictGet('yt_dict', 'value', number + 1) FROM numbers(3)"
        )
        instance.query_and_get_error("SELECT dictGet('yt_dict', 'value', 2)") == "40\n"
    instance.query("DROP DICTIONARY yt_dict")
    yt.remove_table(path)


@pytest.mark.parametrize(
    "dynamic_table",
    [
        (True),
        (False),
    ],
)
def test_yt_dictionary_cache_complex_key(started_cluster, dynamic_table):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    path = "//tmp/table"
    yt.create_table(
        path,
        '{"id1":1, "id2":1, "value":20}{"id1":2, "id2":2, "value":40}{"id1":3, "id2":3, "value":30}',
        sorted_columns=("id1", "id2"),
        schema={"id1": "int32", "id2": "int32", "value": "int32"},
        dynamic=dynamic_table,
    )

    instance.query(
        f"CREATE DICTIONARY yt_dict(id1 Int32, id2 Int32, value Int32) PRIMARY KEY id1, id2 SOURCE(YTSAURUS(http_proxy_urls '{YT_URI}' cypress_path '{path}' oauth_token '{YT_DEFAULT_TOKEN}')) LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 10)) LIFETIME(MIN 0 MAX 1000)"
    )
    if dynamic_table:
        assert (
            instance.query(
                "SELECT dictGet('yt_dict', 'value', (number + 1, number + 1)) FROM numbers(3)"
            )
            == "20\n40\n30\n"
        )
        assert instance.query("SELECT dictGet('yt_dict', 'value', (2, 2))") == "40\n"
    else:
        instance.query_and_get_error(
            "SELECT dictGet('yt_dict', 'value', (number + 1, number + 1)) FROM numbers(3)"
        ) == "20\n40\n30\n"
        instance.query_and_get_error(
            "SELECT dictGet('yt_dict', 'value', (2, 2))"
        ) == "40\n"

    instance.query("DROP DICTIONARY yt_dict")
    yt.remove_table(path)


def test_yt_dictionary_multiple_enpoints(started_cluster):
    yt = YTsaurusCLI(started_cluster, instance, YT_HOST, YT_PORT)
    path = "//tmp/table"

    yt.create_table(
        path,
        '{"id":1,"value":20}{"id":2,"value":40}{"id":3,"value":30}',
        schema={"id": "int32", "value": "int32"},
        dynamic=False,
    )
    instance.query(
        f"CREATE DICTIONARY yt_dict(id UInt64, value Int32) PRIMARY KEY id SOURCE(YTSAURUS(http_proxy_urls 'http://incorrect_endpoint|{YT_URI}' cypress_path '{path}' oauth_token '{YT_DEFAULT_TOKEN}' check_table_schema 0)) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);"
    )
    assert (
        instance.query(
            "SELECT dictGet('yt_dict', 'value', number + 1) FROM numbers(3) SETTINGS http_max_tries = 10, http_retry_max_backoff_ms=2000"
        )
        == "20\n40\n30\n"
    )
    assert (
        instance.query(
            "SELECT dictGet('yt_dict', 'value', 2) SETTINGS http_max_tries = 10, http_retry_max_backoff_ms=2000"
        )
        == "40\n"
    )

    instance.query("DROP DICTIONARY yt_dict")
    yt.remove_table(path)
