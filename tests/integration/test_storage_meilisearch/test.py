import json
import os
from time import sleep
import meilisearch
from pymysql import NULL

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            "meili", main_configs=["configs/named_collection.xml"], with_meili=True
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_meili_client(started_cluster):
    connection_str = "http://localhost:{}".format(started_cluster.meili_port)
    return meilisearch.Client(connection_str)


def get_meili_secure_client(started_cluster):
    connection_str = "http://localhost:{}".format(started_cluster.meili_secure_port)
    return meilisearch.Client(connection_str, "password")


def push_data(client, table, documents):
    ans = table.add_documents(documents)
    client.wait_for_task(ans["uid"])


def push_movies(client):
    print(SCRIPT_DIR + "/movies.json")
    json_file = open(SCRIPT_DIR + "/movies.json")
    movies = json.load(json_file)
    ans = client.index("movies").add_documents(movies)
    client.wait_for_task(ans["uid"], 100000)


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_simple_select(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i)})

    push_data(client, table, data)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE simple_meili_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili1:7700', 'new_table', '')"
    )

    assert node.query("SELECT COUNT() FROM simple_meili_table") == "100\n"
    assert (
        node.query("SELECT sum(id) FROM simple_meili_table")
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query("SELECT data FROM simple_meili_table WHERE id = 42")
        == hex(42 * 42) + "\n"
    )
    node.query("DROP TABLE simple_meili_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_insert(started_cluster):
    client = get_meili_client(started_cluster)
    new_table = client.index("new_table")
    big_table = client.index("big_table")

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE new_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili1:7700', 'new_table', '')"
    )

    node.query(
        "INSERT INTO new_table (id, data) VALUES (1, '1') (2, '2') (3, '3') (4, '4') (5, '5') (6, '6') (7, '7')"
    )
    sleep(1)
    assert len(new_table.get_documents()) == 7

    node.query(
        "CREATE TABLE big_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili1:7700', 'big_table', '')"
    )

    values = ""
    for i in range(1, 40001):
        values += "(" + str(i) + ", " + "'" + str(i) + "'" + ") "

    node.query("INSERT INTO big_table (id, data) VALUES " + values)
    sleep(5)
    ans = big_table.update_sortable_attributes(["id"])
    client.wait_for_task(ans["uid"])
    docs = big_table.get_documents({"limit": 40010})
    assert len(docs) == 40000

    node.query("DROP TABLE new_table")
    node.query("DROP TABLE big_table")
    new_table.delete()
    big_table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_meilimatch(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("movies")
    table.update_sortable_attributes(["release_date"])
    table.update_filterable_attributes(["release_date"])

    push_movies(client)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE movies_table(id String, title String, release_date Int64) ENGINE = MeiliSearch('http://meili1:7700', 'movies', '')"
    )

    assert node.query("SELECT COUNT() FROM movies_table") == "19546\n"

    real_json = table.search(
        "abaca",
        {"attributesToRetrieve": ["id", "title", "release_date"], "limit": 20000},
    )["hits"]
    click_ans = (
        "["
        + ", ".join(
            node.query(
                'SELECT * FROM movies_table WHERE \
                                            meiliMatch(\'"q"="abaca"\') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0'
            ).split("\n")[:-1]
        )
        + "]"
    )
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search(
        "abaca",
        {
            "attributesToRetrieve": ["id", "title", "release_date"],
            "limit": 20000,
            "sort": ["release_date:asc"],
        },
    )["hits"]
    click_ans = (
        "["
        + ", ".join(
            node.query(
                'SELECT * FROM movies_table WHERE \
                                            meiliMatch(\'"q"="abaca"\', \'"sort"=["release_date:asc"]\') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0'
            ).split("\n")[:-1]
        )
        + "]"
    )
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search(
        "abaca",
        {
            "attributesToRetrieve": ["id", "title", "release_date"],
            "limit": 20000,
            "sort": ["release_date:desc"],
            "filter": "release_date < 700000000",
        },
    )["hits"]
    click_ans = (
        "["
        + ", ".join(
            node.query(
                'SELECT * FROM movies_table WHERE \
                                            meiliMatch(\'"q"="abaca"\', \'"sort"=["release_date:asc"]\', \'"filter"="release_date < 700000000"\') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0'
            ).split("\n")[:-1]
        )
        + "]"
    )
    click_json = json.loads(click_ans)
    assert real_json == click_json

    node.query("DROP TABLE movies_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_incorrect_data_type(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i), "aaaa": "Hello"})

    push_data(client, table, data)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE strange_meili_table(id UInt64, data String, bbbb String) ENGINE = MeiliSearch('http://meili1:7700', 'new_table', '')"
    )

    error = node.query_and_get_error("SELECT bbbb FROM strange_meili_table")
    assert "MEILISEARCH_MISSING_SOME_COLUMNS" in error

    node.query("DROP TABLE strange_meili_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_simple_select_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i)})

    push_data(client, table, data)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE simple_meili_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili_secure:7700', 'new_table', 'password')"
    )

    node.query(
        "CREATE TABLE wrong_meili_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili_secure:7700', 'new_table', 'wrong_password')"
    )

    assert node.query("SELECT COUNT() FROM simple_meili_table") == "100\n"
    assert (
        node.query("SELECT sum(id) FROM simple_meili_table")
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query("SELECT data FROM simple_meili_table WHERE id = 42")
        == hex(42 * 42) + "\n"
    )

    error = node.query_and_get_error("SELECT COUNT() FROM wrong_meili_table")
    assert "MEILISEARCH_EXCEPTION" in error

    error = node.query_and_get_error("SELECT sum(id) FROM wrong_meili_table")
    assert "MEILISEARCH_EXCEPTION" in error

    error = node.query_and_get_error("SELECT data FROM wrong_meili_table WHERE id = 42")
    assert "MEILISEARCH_EXCEPTION" in error

    node.query("DROP TABLE simple_meili_table")
    node.query("DROP TABLE wrong_meili_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_meilimatch_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    table = client.index("movies")
    table.update_sortable_attributes(["release_date"])
    table.update_filterable_attributes(["release_date"])

    push_movies(client)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE movies_table(id String, title String, release_date Int64) ENGINE = MeiliSearch('http://meili_secure:7700', 'movies', 'password')"
    )

    assert node.query("SELECT COUNT() FROM movies_table") == "19546\n"

    real_json = table.search(
        "abaca",
        {"attributesToRetrieve": ["id", "title", "release_date"], "limit": 20000},
    )["hits"]
    click_ans = (
        "["
        + ", ".join(
            node.query(
                'SELECT * FROM movies_table WHERE \
                                            meiliMatch(\'"q"="abaca"\') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0'
            ).split("\n")[:-1]
        )
        + "]"
    )
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search(
        "abaca",
        {
            "attributesToRetrieve": ["id", "title", "release_date"],
            "limit": 20000,
            "sort": ["release_date:asc"],
        },
    )["hits"]
    click_ans = (
        "["
        + ", ".join(
            node.query(
                'SELECT * FROM movies_table WHERE \
                                            meiliMatch(\'"q"="abaca"\', \'"sort"=["release_date:asc"]\') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0'
            ).split("\n")[:-1]
        )
        + "]"
    )
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search(
        "abaca",
        {
            "attributesToRetrieve": ["id", "title", "release_date"],
            "limit": 20000,
            "sort": ["release_date:desc"],
            "filter": "release_date < 700000000",
        },
    )["hits"]
    click_ans = (
        "["
        + ", ".join(
            node.query(
                'SELECT * FROM movies_table WHERE \
                                            meiliMatch(\'"q"="abaca"\', \'"sort"=["release_date:asc"]\', \'"filter"="release_date < 700000000"\') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0'
            ).split("\n")[:-1]
        )
        + "]"
    )
    click_json = json.loads(click_ans)
    assert real_json == click_json

    node.query("DROP TABLE movies_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_incorrect_data_type_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i), "aaaa": "Hello"})

    push_data(client, table, data)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE strange_meili_table(id UInt64, data String, bbbb String) ENGINE = MeiliSearch('http://meili_secure:7700', 'new_table', 'password')"
    )

    error = node.query_and_get_error("SELECT bbbb FROM strange_meili_table")
    assert "MEILISEARCH_MISSING_SOME_COLUMNS" in error

    node.query("DROP TABLE strange_meili_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_insert_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    new_table = client.index("new_table")
    big_table = client.index("big_table")

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE new_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili_secure:7700', 'new_table', 'password')"
    )

    node.query(
        "INSERT INTO new_table (id, data) VALUES (1, '1') (2, '2') (3, '3') (4, '4') (5, '5') (6, '6') (7, '7')"
    )
    sleep(1)
    assert len(new_table.get_documents()) == 7

    node.query(
        "CREATE TABLE big_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili_secure:7700', 'big_table', 'password')"
    )

    values = ""
    for i in range(1, 40001):
        values += "(" + str(i) + ", " + "'" + str(i) + "'" + ") "

    node.query("INSERT INTO big_table (id, data) VALUES " + values)
    sleep(5)
    ans = big_table.update_sortable_attributes(["id"])
    client.wait_for_task(ans["uid"])
    docs = big_table.get_documents({"limit": 40010})
    assert len(docs) == 40000

    node.query("DROP TABLE new_table")
    node.query("DROP TABLE big_table")
    new_table.delete()
    big_table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_security_levels(started_cluster):
    client = get_meili_secure_client(started_cluster)
    new_table = client.index("new_table")
    search_key = client.get_keys()["results"][0]["key"]
    admin_key = client.get_keys()["results"][1]["key"]

    values = ""
    for i in range(1, 101):
        values += "(" + str(i) + ", " + "'" + str(i) + "'" + ") "

    node = started_cluster.instances["meili"]
    node.query(
        f"CREATE TABLE read_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili_secure:7700', 'new_table', '{search_key}')"
    )
    node.query(
        f"CREATE TABLE write_table(id UInt64, data String) ENGINE = MeiliSearch('http://meili_secure:7700', 'new_table', '{admin_key}')"
    )

    error = node.query_and_get_error(
        "INSERT INTO read_table (id, data) VALUES " + values
    )
    assert "MEILISEARCH_EXCEPTION" in error

    node.query("INSERT INTO write_table (id, data) VALUES " + values)
    sleep(1)
    assert len(new_table.get_documents({"limit": 40010})) == 100

    ans1 = (
        "["
        + ", ".join(
            node.query(
                'SELECT * FROM read_table where meiliMatch(\'"q"=""\') \
                                       format JSONEachRow settings output_format_json_quote_64bit_integers=0'
            ).split("\n")[:-1]
        )
        + "]"
    )
    ans2 = (
        "["
        + ", ".join(
            node.query(
                "SELECT * FROM write_table \
                                       format JSONEachRow settings output_format_json_quote_64bit_integers=0"
            ).split("\n")[:-1]
        )
        + "]"
    )

    assert ans1 == ans2

    docs = json.loads(ans1)
    assert len(docs) == 100

    node.query("DROP TABLE read_table")
    node.query("DROP TABLE write_table")
    client.index("new_table").delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_types(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("types_table")

    data = {
        "id": 1,
        "UInt8_test": 128,
        "UInt16_test": 32768,
        "UInt32_test": 2147483648,
        "UInt64_test": 9223372036854775808,
        "Int8_test": -128,
        "Int16_test": -32768,
        "Int32_test": -2147483648,
        "Int64_test": -9223372036854775808,
        "String_test": "abacaba",
        "Float32_test": 42.42,
        "Float64_test": 42.42,
        "Array_test": [["aba", "caba"], ["2d", "array"]],
        "Null_test1": "value",
        "Null_test2": NULL,
        "Bool_test1": True,
        "Bool_test2": False,
        "Json_test": {"a": 1, "b": {"in_json": "qwerty"}},
    }

    push_data(client, table, data)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE types_table(\
                                        id UInt64,\
                                        UInt8_test UInt8,\
                                        UInt16_test UInt16,\
                                        UInt32_test UInt32,\
                                        UInt64_test UInt64,\
                                        Int8_test Int8,\
                                        Int16_test Int16,\
                                        Int32_test Int32,\
                                        Int64_test Int64,\
                                        String_test String,\
                                        Float32_test Float32,\
                                        Float64_test Float64,\
                                        Array_test Array(Array(String)),\
                                        Null_test1 Nullable(String),\
                                        Null_test2 Nullable(String),\
                                        Bool_test1 Boolean,\
                                        Bool_test2 Boolean,\
                                        Json_test String\
                                        ) ENGINE = MeiliSearch('http://meili1:7700', 'types_table', '')"
    )

    assert node.query("SELECT id FROM types_table") == "1\n"
    assert node.query("SELECT UInt8_test FROM types_table") == "128\n"
    assert node.query("SELECT UInt16_test FROM types_table") == "32768\n"
    assert node.query("SELECT UInt32_test FROM types_table") == "2147483648\n"
    assert node.query("SELECT UInt64_test FROM types_table") == "9223372036854775808\n"
    assert node.query("SELECT Int8_test FROM types_table") == "-128\n"
    assert node.query("SELECT Int16_test FROM types_table") == "-32768\n"
    assert node.query("SELECT Int32_test FROM types_table") == "-2147483648\n"
    assert node.query("SELECT Int64_test FROM types_table") == "-9223372036854775808\n"
    assert node.query("SELECT String_test FROM types_table") == "abacaba\n"
    assert node.query("SELECT Float32_test FROM types_table") == "42.42\n"
    assert node.query("SELECT Float32_test FROM types_table") == "42.42\n"
    assert (
        node.query("SELECT Array_test FROM types_table")
        == "[['aba','caba'],['2d','array']]\n"
    )
    assert node.query("SELECT Null_test1 FROM types_table") == "value\n"
    assert node.query("SELECT Null_test2 FROM types_table") == "NULL\n"
    assert node.query("SELECT Bool_test1 FROM types_table") == "true\n"
    assert node.query("SELECT Bool_test2 FROM types_table") == "false\n"
    assert (
        node.query("SELECT Json_test FROM types_table")
        == '{"a":1,"b":{"in_json":"qwerty"}}\n'
    )

    node.query("DROP TABLE types_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_named_collection(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i)})

    push_data(client, table, data)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE simple_meili_table(id UInt64, data String) ENGINE = MeiliSearch( named_collection_for_meili )"
    )

    assert node.query("SELECT COUNT() FROM simple_meili_table") == "100\n"
    assert (
        node.query("SELECT sum(id) FROM simple_meili_table")
        == str(sum(range(0, 100))) + "\n"
    )

    assert (
        node.query("SELECT data FROM simple_meili_table WHERE id = 42")
        == hex(42 * 42) + "\n"
    )
    node.query("DROP TABLE simple_meili_table")
    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_named_collection_secure(started_cluster):
    client_secure = get_meili_secure_client(started_cluster)
    client_free = get_meili_client(started_cluster)
    table_secure = client_secure.index("new_table")
    table_free = client_free.index("new_table")

    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i)})

    push_data(client_secure, table_secure, data)
    push_data(client_free, table_free, data)

    node = started_cluster.instances["meili"]
    node.query(
        "CREATE TABLE simple_meili_table(id UInt64, data String) ENGINE = MeiliSearch( named_collection_for_meili_secure )"
    )

    node.query(
        "CREATE TABLE wrong_meili_table(id UInt64, data String) ENGINE = MeiliSearch( named_collection_for_meili_secure_no_password )"
    )

    node.query(
        'CREATE TABLE combine_meili_table(id UInt64, data String) ENGINE = MeiliSearch( named_collection_for_meili_secure_no_password, password="password" )'
    )

    assert node.query("SELECT COUNT() FROM simple_meili_table") == "100\n"
    assert (
        node.query("SELECT sum(id) FROM simple_meili_table")
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query("SELECT data FROM simple_meili_table WHERE id = 42")
        == hex(42 * 42) + "\n"
    )

    assert node.query("SELECT COUNT() FROM combine_meili_table") == "100\n"
    assert (
        node.query("SELECT sum(id) FROM combine_meili_table")
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query("SELECT data FROM combine_meili_table WHERE id = 42")
        == hex(42 * 42) + "\n"
    )

    error = node.query_and_get_error("SELECT COUNT() FROM wrong_meili_table")
    assert "MEILISEARCH_EXCEPTION" in error

    error = node.query_and_get_error("SELECT sum(id) FROM wrong_meili_table")
    assert "MEILISEARCH_EXCEPTION" in error

    error = node.query_and_get_error("SELECT data FROM wrong_meili_table WHERE id = 42")
    assert "MEILISEARCH_EXCEPTION" in error

    node.query("DROP TABLE simple_meili_table")
    node.query("DROP TABLE wrong_meili_table")
    node.query("DROP TABLE combine_meili_table")
    table_secure.delete()
    table_free.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_table_function(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i)})

    push_data(client, table, data)

    node = started_cluster.instances["meili"]

    assert (
        node.query(
            "SELECT COUNT() FROM MeiliSearch('http://meili1:7700', 'new_table', '')"
        )
        == "100\n"
    )
    assert (
        node.query(
            "SELECT sum(id) FROM MeiliSearch('http://meili1:7700', 'new_table', '')"
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            "SELECT data FROM MeiliSearch('http://meili1:7700', 'new_table', '') WHERE id = 42"
        )
        == hex(42 * 42) + "\n"
    )

    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_table_function_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({"id": i, "data": hex(i * i)})

    push_data(client, table, data)

    node = started_cluster.instances["meili"]

    assert (
        node.query(
            "SELECT COUNT() FROM MeiliSearch('http://meili_secure:7700', 'new_table', 'password')"
        )
        == "100\n"
    )
    assert (
        node.query(
            "SELECT sum(id) FROM MeiliSearch('http://meili_secure:7700', 'new_table', 'password')"
        )
        == str(sum(range(0, 100))) + "\n"
    )
    assert (
        node.query(
            "SELECT data FROM MeiliSearch('http://meili_secure:7700', 'new_table', 'password') WHERE id = 42"
        )
        == hex(42 * 42) + "\n"
    )

    error = node.query_and_get_error(
        "SELECT COUNT() FROM MeiliSearch('http://meili_secure:7700', 'new_table', 'wrong_password')"
    )
    assert "MEILISEARCH_EXCEPTION" in error

    error = node.query_and_get_error(
        "SELECT sum(id) FROM MeiliSearch('http://meili_secure:7700', 'new_table', 'wrong_password')"
    )
    assert "MEILISEARCH_EXCEPTION" in error

    error = node.query_and_get_error(
        "SELECT data FROM MeiliSearch('http://meili_secure:7700', 'new_table', 'wrong_password') WHERE id = 42"
    )
    assert "MEILISEARCH_EXCEPTION" in error

    table.delete()


@pytest.mark.parametrize("started_cluster", [False], indirect=["started_cluster"])
def test_types_in_table_function(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("types_table")

    data = {
        "id": 1,
        "UInt8_test": 128,
        "UInt16_test": 32768,
        "UInt32_test": 2147483648,
        "Int8_test": -128,
        "Int16_test": -32768,
        "Int32_test": -2147483648,
        "Int64_test": -9223372036854775808,
        "String_test": "abacaba",
        "Float32_test": 42.42,
        "Float64_test": 42.42,
        "Array_test": [["aba", "caba"], ["2d", "array"]],
        "Null_test1": "value",
        "Null_test2": NULL,
        "Bool_test1": True,
        "Bool_test2": False,
        "Json_test": {"a": 1, "b": {"in_json": "qwerty"}},
    }

    push_data(client, table, data)

    node = started_cluster.instances["meili"]

    assert (
        node.query(
            "SELECT id FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT UInt8_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "128\n"
    )
    assert (
        node.query(
            "SELECT UInt16_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "32768\n"
    )
    assert (
        node.query(
            "SELECT UInt32_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "2147483648\n"
    )
    assert (
        node.query(
            "SELECT Int8_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "-128\n"
    )
    assert (
        node.query(
            "SELECT Int16_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "-32768\n"
    )
    assert (
        node.query(
            "SELECT Int32_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "-2147483648\n"
    )
    assert (
        node.query(
            "SELECT Int64_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "-9223372036854775808\n"
    )
    assert (
        node.query(
            "SELECT String_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "abacaba\n"
    )
    assert (
        node.query(
            "SELECT Float32_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "42.42\n"
    )
    assert (
        node.query(
            "SELECT Float32_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "42.42\n"
    )
    assert (
        node.query(
            "SELECT Array_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "[['aba','caba'],['2d','array']]\n"
    )
    assert (
        node.query(
            "SELECT Null_test1 FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "value\n"
    )
    assert (
        node.query(
            "SELECT Null_test2 FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "NULL\n"
    )
    assert (
        node.query(
            "SELECT Bool_test1 FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "1\n"
    )
    assert (
        node.query(
            "SELECT Bool_test2 FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == "0\n"
    )
    assert (
        node.query(
            "SELECT Json_test FROM MeiliSearch('http://meili1:7700', 'types_table', '')"
        )
        == '{"a":1,"b":{"in_json":"qwerty"}}\n'
    )

    table.delete()
