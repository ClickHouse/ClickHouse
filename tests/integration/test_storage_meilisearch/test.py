import json
import os
from time import sleep
import meilisearch

import pytest
from helpers.client import QueryRuntimeException

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance('meili',
                                    with_meili=True)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_meili_client(started_cluster):
    connection_str = 'http://localhost:{}'.format(started_cluster.meili_port)
    return meilisearch.Client(connection_str)

def get_meili_secure_client(started_cluster):
    connection_str = 'http://localhost:{}'.format(started_cluster.meili_secure_port)
    return meilisearch.Client(connection_str, "password")

def push_data(client, table, documents):
    ans = table.add_documents(documents)
    client.wait_for_task(ans['uid'])

def push_movies(client):
    print(SCRIPT_DIR + '/movies.json')
    json_file = open(SCRIPT_DIR + '/movies.json')
    movies = json.load(json_file)
    ans = client.index('movies').add_documents(movies)
    client.wait_for_task(ans['uid'], 100000)


@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_simple_select(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({'id': i, 'data': hex(i * i)})

    push_data(client, table, data)

    node = started_cluster.instances['meili']
    node.query(
        "CREATE TABLE simple_meili_table(id UInt64, data String) ENGINE = MeiliSearch('meili1:7700', 'new_table', '')")

    assert node.query("SELECT COUNT() FROM simple_meili_table") == '100\n'
    assert node.query("SELECT sum(id) FROM simple_meili_table") == str(sum(range(0, 100))) + '\n'

    assert node.query("SELECT data FROM simple_meili_table WHERE id = 42") == hex(42 * 42) + '\n'
    node.query("DROP TABLE simple_meili_table")
    table.delete()


@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_insert(started_cluster):
    client = get_meili_client(started_cluster)
    new_table = client.index("new_table")
    big_table = client.index("big_table")
    
    node = started_cluster.instances['meili']
    node.query("CREATE TABLE new_table(id UInt64, data String) ENGINE = MeiliSearch('meili1:7700', 'new_table', '')")

    node.query("INSERT INTO new_table (id, data) VALUES (1, '1') (2, '2') (3, '3') (4, '4') (5, '5') (6, '6') (7, '7')")
    sleep(1)
    assert len(new_table.get_documents()) == 7

    node.query("CREATE TABLE big_table(id UInt64, data String) ENGINE = MeiliSearch('meili1:7700', 'big_table', '')")

    values = ""
    for i in range(1, 40001):
        values += "(" + str(i) + ", " + "\'" + str(i) + "\'" + ") "
    
    node.query("INSERT INTO big_table (id, data) VALUES " + values)
    sleep(5)
    ans = big_table.update_sortable_attributes(['id'])
    client.wait_for_task(ans['uid'])
    docs = big_table.search("", {"limit":50000, 'sort': ['id:asc']})["hits"]
    assert len(docs) == 40000

    for i in range(1, 40001):
        assert docs[i - 1] == {"id": i, "data": str(i)}

    node.query("DROP TABLE new_table")
    node.query("DROP TABLE big_table")
    new_table.delete()
    big_table.delete()


@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_meilimatch(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("movies")
    table.update_sortable_attributes(['release_date'])
    table.update_filterable_attributes(['release_date'])

    push_movies(client)

    node = started_cluster.instances['meili']
    node.query("CREATE TABLE movies_table(id String, title String, release_date Int64) ENGINE = MeiliSearch('meili1:7700', 'movies', '')")

    assert node.query("SELECT COUNT() FROM movies_table") == '19546\n'
    assert node.query("SELECT COUNT() FROM movies_table WHERE meiliMatch('\"q\"=\"abaca\"')") == '13\n'
    assert node.query("SELECT sum(release_date) FROM movies_table WHERE meiliMatch('\"q\"=\"abaca\"')") == '12887532000\n'

    real_json = table.search("abaca", {"attributesToRetrieve":["id", "title", "release_date"], "limit":20000})["hits"]
    click_ans = "[" + ", ".join(node.query("SELECT * FROM movies_table WHERE \
                                            meiliMatch('\"q\"=\"abaca\"') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search("abaca", {"attributesToRetrieve":["id", "title", "release_date"], "limit":20000, 'sort': ['release_date:asc']})["hits"]
    click_ans = "[" + ", ".join(node.query("SELECT * FROM movies_table WHERE \
                                            meiliMatch('\"q\"=\"abaca\"', '\"sort\"=[\"release_date:asc\"]') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search("abaca", {"attributesToRetrieve":["id", "title", "release_date"], "limit":20000, 'sort': ['release_date:desc'], 'filter': 'release_date < 700000000'})["hits"]
    click_ans = "[" + ", ".join(node.query("SELECT * FROM movies_table WHERE \
                                            meiliMatch('\"q\"=\"abaca\"', '\"sort\"=[\"release_date:asc\"]', '\"filter\"=\"release_date < 700000000\"') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"
    click_json = json.loads(click_ans)
    assert real_json == click_json


    node.query("DROP TABLE movies_table")
    table.delete()


@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_incorrect_data_type(started_cluster):
    client = get_meili_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({'id': i, 'data': hex(i * i), 'aaaa': 'Hello'})

    push_data(client, table, data)   
    
    node = started_cluster.instances['meili']
    node.query("CREATE TABLE strange_meili_table(id UInt64, data String, bbbb String) ENGINE = MeiliSearch('meili1:7700', 'new_table', '')")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT bbbb FROM strange_meili_table")

    node.query("DROP TABLE strange_meili_table")
    table.delete()


@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_simple_select_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({'id': i, 'data': hex(i * i)})

    push_data(client, table, data)

    node = started_cluster.instances['meili']
    node.query(
        "CREATE TABLE simple_meili_table(id UInt64, data String) ENGINE = MeiliSearch('meili_secure:7700', 'new_table', 'password')")

    node.query(
        "CREATE TABLE wrong_meili_table(id UInt64, data String) ENGINE = MeiliSearch('meili_secure:7700', 'new_table', 'wrong_password')")

    assert node.query("SELECT COUNT() FROM simple_meili_table") == '100\n'
    assert node.query("SELECT sum(id) FROM simple_meili_table") == str(sum(range(0, 100))) + '\n'
    assert node.query("SELECT data FROM simple_meili_table WHERE id = 42") == hex(42 * 42) + '\n'

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM wrong_meili_table")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT sum(id) FROM wrong_meili_table")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT data FROM wrong_meili_table WHERE id = 42")

    node.query("DROP TABLE simple_meili_table")
    node.query("DROP TABLE wrong_meili_table")
    table.delete()



@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_meilimatch_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    table = client.index("movies")
    table.update_sortable_attributes(['release_date'])
    table.update_filterable_attributes(['release_date'])

    push_movies(client)

    node = started_cluster.instances['meili']
    node.query("CREATE TABLE movies_table(id String, title String, release_date Int64) ENGINE = MeiliSearch('meili_secure:7700', 'movies', 'password')")

    assert node.query("SELECT COUNT() FROM movies_table") == '19546\n'
    assert node.query("SELECT COUNT() FROM movies_table WHERE meiliMatch('\"q\"=\"abaca\"')") == '13\n'
    assert node.query("SELECT sum(release_date) FROM movies_table WHERE meiliMatch('\"q\"=\"abaca\"')") == '12887532000\n'

    real_json = table.search("abaca", {"attributesToRetrieve":["id", "title", "release_date"], "limit":20000})["hits"]
    click_ans = "[" + ", ".join(node.query("SELECT * FROM movies_table WHERE \
                                            meiliMatch('\"q\"=\"abaca\"') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search("abaca", {"attributesToRetrieve":["id", "title", "release_date"], "limit":20000, 'sort': ['release_date:asc']})["hits"]
    click_ans = "[" + ", ".join(node.query("SELECT * FROM movies_table WHERE \
                                            meiliMatch('\"q\"=\"abaca\"', '\"sort\"=[\"release_date:asc\"]') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"
    click_json = json.loads(click_ans)
    assert real_json == click_json

    real_json = table.search("abaca", {"attributesToRetrieve":["id", "title", "release_date"], "limit":20000, 'sort': ['release_date:desc'], 'filter': 'release_date < 700000000'})["hits"]
    click_ans = "[" + ", ".join(node.query("SELECT * FROM movies_table WHERE \
                                            meiliMatch('\"q\"=\"abaca\"', '\"sort\"=[\"release_date:asc\"]', '\"filter\"=\"release_date < 700000000\"') \
                                            format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"
    click_json = json.loads(click_ans)
    assert real_json == click_json


    node.query("DROP TABLE movies_table")
    table.delete()


@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_incorrect_data_type_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    table = client.index("new_table")
    data = []
    for i in range(0, 100):
        data.append({'id': i, 'data': hex(i * i), 'aaaa': 'Hello'})

    push_data(client, table, data)   
    
    node = started_cluster.instances['meili']
    node.query("CREATE TABLE strange_meili_table(id UInt64, data String, bbbb String) ENGINE = MeiliSearch('meili_secure:7700', 'new_table', 'password')")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT bbbb FROM strange_meili_table")

    node.query("DROP TABLE strange_meili_table")
    table.delete()

@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_insert_secure(started_cluster):
    client = get_meili_secure_client(started_cluster)
    new_table = client.index("new_table")
    big_table = client.index("big_table")
    
    node = started_cluster.instances['meili']
    node.query("CREATE TABLE new_table(id UInt64, data String) ENGINE = MeiliSearch('meili_secure:7700', 'new_table', 'password')")

    node.query("INSERT INTO new_table (id, data) VALUES (1, '1') (2, '2') (3, '3') (4, '4') (5, '5') (6, '6') (7, '7')")
    sleep(1)
    assert len(new_table.get_documents()) == 7

    node.query("CREATE TABLE big_table(id UInt64, data String) ENGINE = MeiliSearch('meili_secure:7700', 'big_table', 'password')")

    values = ""
    for i in range(1, 40001):
        values += "(" + str(i) + ", " + "\'" + str(i) + "\'" + ") "
    
    node.query("INSERT INTO big_table (id, data) VALUES " + values)
    sleep(5)
    ans = big_table.update_sortable_attributes(['id'])
    client.wait_for_task(ans['uid'])
    docs = big_table.search("", {"limit":50000, 'sort': ['id:asc']})["hits"]
    assert len(docs) == 40000

    for i in range(1, 40001):
        assert docs[i - 1] == {"id": i, "data": str(i)}

    node.query("DROP TABLE new_table")
    node.query("DROP TABLE big_table")
    new_table.delete()
    big_table.delete()

@pytest.mark.parametrize('started_cluster', [False], indirect=['started_cluster'])
def test_security_levels(started_cluster):
    client = get_meili_secure_client(started_cluster)
    search_key = client.get_keys()['results'][0]['key']
    admin_key = client.get_keys()['results'][1]['key']
    
    values = ""
    for i in range(1, 101):
        values += "(" + str(i) + ", " + "\'" + str(i) + "\'" + ") "

    node = started_cluster.instances['meili']
    node.query(f"CREATE TABLE read_table(id UInt64, data String) ENGINE = MeiliSearch('meili_secure:7700', 'new_table', '{search_key}')")
    node.query(f"CREATE TABLE write_table(id UInt64, data String) ENGINE = MeiliSearch('meili_secure:7700', 'new_table', '{admin_key}')")

    with pytest.raises(QueryRuntimeException):
        node.query("INSERT INTO read_table (id, data) VALUES " + values)

    node.query("INSERT INTO write_table (id, data) VALUES " + values)
    sleep(1)

    ans1 = "[" + ", ".join(node.query("SELECT * FROM read_table \
                                       format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"
    ans2 = "[" + ", ".join(node.query("SELECT * FROM write_table \
                                       format JSONEachRow settings output_format_json_quote_64bit_integers=0").split("\n")[:-1]) + "]"

    assert ans1 == ans2

    docs = json.loads(ans1)
    assert len(docs) == 100

    node.query("DROP TABLE read_table")
    node.query("DROP TABLE write_table")
    client.index("new_table").delete()

