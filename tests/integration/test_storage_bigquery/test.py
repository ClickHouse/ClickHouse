import json
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
)

MOCK_PORT = 8938
BASE_URL = f"http://localhost:{MOCK_PORT}"

PROJECT = "test-project"
DATASET = "test_dataset"
ACCESS_TOKEN = "test-static-token"

SA_CLIENT_EMAIL = "tester@example-project.iam.gserviceaccount.com"

# A throwaway RSA key used only for signing JWTs to the mock server in this test.
SA_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDNSu+yK2bcH371
np3hDra82OUdTMcNfVEFveMX4W+5pxsD42o2GUv1LzyfHa9SFdgd9ST5O30OdDid
6JYHD/BETY630HblsZnbr0VXqzfouf6oCYczIxfww0/FniKiosjV3wnr7N2xNiHk
kx1AIYxG9b9TVv0IL7KCAIsQF6Wvbpd+/o6780We40w5s67H+DBg2irYdEXiROrb
QTBT9hJ4aAohxEe40indd/EztOrgYINcsCeO09t6qT435p+5EDtQ5DFVIyIFV4aK
sb/XbrG/WXg8l4TKBBMVX6aU+7f5Gbv2ZopTJ9QGPBt6WHiDixltjO0bXfdx3f1s
jytZkzKPAgMBAAECggEABGM2SD/hN5HSFtHIXLJXJCYb5MpqnqvaUB3E0r0rXskR
ghMZhq/NDcpv/elBK/yc3Sxgw+rfkLAvMutw+WmqdZXqwkb0Tj3Degx+65Fp5SbE
feGQ6tOJ2XLIgQK7iW+ijIpIc1W2pJOcRH8DFbHMhl+43RZzhfnu/hRLjzvSJgj/
QF4Jgc87kharfkDd6bd69akQmFrhK8j7h80+q774orESRBTClWKZPdcsLMjGYkG4
MD00cuGIyNQv/uZvvmcZY2qAJdZfLYfrEFYLpZciBftibculAVtc/kFZ1EUn4XAg
u4105xXrAaX452sd33RYNbiC5qUREycvt7GwDHjEYQKBgQDojSoDkz/TAlVbqggd
Y63uOk5sDs64z+OMT99/WPihoC6SXmjA/NTrnlKhJwz0r5fp9b8vh8C/YumLb8ek
Yeywha5TYlw+5tM0B1hBi+ygSth7QAUCxqT/dCi188jaD2xc9uy4it3qCqmlaVFC
Berc0ERSJjEkZXrfMHSAlcumhwKBgQDh/iUbdBxZZDegdgQ98SJKeKqGmbwt6Oyk
tOlXm4cO0dH3fdbV6H2tSmbdCLSKxuFAtIv0PBTBQCVpWmFFRRLD97Rscn/JbWUk
h8bqMiSurWSF2TkrcNgg2+HA7126tmDR9TTFYHJ43cjTZACE2wrp/YhW4LX0B1f+
Jmb9eDUNuQKBgEmeXr2RAPSA2ZrSIg2Dv80Z4jetHF4/Wa7SnlNHTDaGahfmEU1+
/Ly2ezwPC7fuWq4zINogJLGx03NT6KSuZ2qed0hobAFxu0zOQm9Fp5w69XtXEf8F
+bfxAu5PBbeaiFiJxvjI5WWxCHGX/KRlESvkNqy28HmwMoyzXz5RrY3DAoGAA7T6
gNfHu0OkkfI9oZJ6AIS4L1sINiyS7SS2iyRW88xHSGr9Aic9IIGO7GM/KuOWQEx1
2zy9Dmpx8qdz3wICC8rdX8YFCJXNyeqcPa0y4tafRn7IrEi+ktNogZSrket57Re7
lN0/I3Jn3+fNBmDdbfclrF8lPOp97AJPQCsfm6kCgYACEAvSv1po7LYB6s7LAdmk
h+3ZkPXqxpD/gtjf7Fml0xoYehePvGEbjEwTA8B8XqO2RbdzQoFmpq7mSt/7Esz6
obhuLb3Sz7+E1Wxx5RJY9tWQ77zaEq7xWeHtc/ahCTOkYB2h4j3if9pB5TkFTggG
Fi+vmHCH4O3yRMexoG4CYA==
-----END PRIVATE KEY-----
"""


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        start_mock_servers(
            cluster,
            os.path.dirname(__file__),
            [("bigquery_mock_server.py", "node", MOCK_PORT)],
        )
        yield cluster
    finally:
        cluster.shutdown()


def mock_ctl(path):
    return cluster.exec_in_container(
        cluster.get_container_id("node"), ["curl", "-s", f"{BASE_URL}{path}"]
    )


def mock_stats():
    return json.loads(mock_ctl("/__stats__"))


def mock_reset():
    mock_ctl("/__reset__")


def sql_str(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def bq(table, creds=f"access_token = '{ACCESS_TOKEN}'"):
    args = f"'{PROJECT}', '{DATASET}', '{table}', base_url = '{BASE_URL}'"
    if creds:
        args += ", " + creds
    return f"bigquery({args})"


def service_account_key(client_email=SA_CLIENT_EMAIL):
    return json.dumps(
        {
            "type": "service_account",
            "client_email": client_email,
            "private_key": SA_PRIVATE_KEY,
            "token_uri": f"{BASE_URL}/token",
        }
    )


def test_schema_inference():
    result = node.query(f"DESCRIBE TABLE {bq('test_types')}")
    name_to_type = dict(line.split("\t")[:2] for line in result.strip().split("\n"))
    assert name_to_type == {
        "i": "Int64",
        "fl": "Nullable(Float64)",
        "s": "Nullable(String)",
        "bin": "Nullable(String)",
        "flag": "Nullable(Bool)",
        "d": "Nullable(Date32)",
        "t": "Nullable(Time64(6))",
        "dt": "Nullable(DateTime64(6, 'UTC'))",
        "ts": "Nullable(DateTime64(6, 'UTC'))",
        "num": "Nullable(Decimal(38, 9))",
        "bignum": "Nullable(Decimal(76, 38))",
        "num_p": "Nullable(Decimal(10, 2))",
        "geo": "Nullable(String)",
        "j": "Nullable(String)",
        "arr": "Array(Int64)",
        "rec": "Tuple(\n    x Nullable(Int64),\n    y String,\n    tags Array(String))",
        "recs": "Array(Tuple(\n    k Int64,\n    val Nullable(String)))",
    }


def test_select_all_types():
    result = node.query(f"SELECT * FROM {bq('test_types')} ORDER BY i FORMAT TSV")
    expected = (
        "1\t1.5\thello\tbinary-data\ttrue\t2024-01-02\t03:04:05.123456\t"
        "2024-01-02 03:04:05.123456\t2024-01-02 03:04:05.123456\t12345.123456789\t"
        '1234567890.12345678901234567890123456789012345678\t12345678.99\tPOINT(1 2)\t{"a":1}\t'
        "[1,2,3]\t(7,'seven',['t1','t2'])\t[(1,'one'),(2,NULL)]\n"
        "2\tinf\t\t\\N\tfalse\t\\N\t\\N\t\\N\t\\N\t-0.000000001\t\\N\t\\N\t\\N\t\\N\t"
        "[]\t(NULL,'',[])\t[]\n"
        "3\tnan\t\\N\tпривет\t\\N\t1970-01-01\t23:59:59.000000\t2299-12-31 23:59:59.000000\t"
        "1970-01-01 00:00:00.000000\t\\N\t"
        "-99999999999999999999999999999999999999.99999999999999999999999999999999999999\t0.01\t\\N\t"
        "[1,2,3]\t[-1]\t(NULL,'y-only',[])\t[]\n"
    )
    assert result == expected


def test_count_and_paging():
    mock_reset()
    assert node.query(f"SELECT count(), sum(i) FROM {bq('test_paging')}") == "10\t45\n"
    # The mock caps pages at 4 rows, so reading 10 rows takes 3 requests.
    assert len(mock_stats()["data_requests"]) == 3


def test_selected_fields():
    mock_reset()
    # The columns are requested in reverse order on purpose: the response returns them
    # in the schema order, and ClickHouse must reorder them for the query.
    result = node.query(f"SELECT s, i FROM {bq('test_paging')} WHERE i < 2 ORDER BY i")
    assert result == "value0\t0\nvalue1\t1\n"

    requests = mock_stats()["data_requests"]
    assert all(r["params"]["selectedFields"] == "i,s" for r in requests)

    mock_reset()
    result = node.query(f"SELECT sum(i) FROM {bq('test_paging')}")
    assert result == "45\n"
    requests = mock_stats()["data_requests"]
    assert all(r["params"]["selectedFields"] == "i" for r in requests)


def test_insert_roundtrip():
    mock_reset()
    node.query(f"""
        INSERT INTO FUNCTION {bq('writable')} VALUES
        (1, 'row1', 1.25, true, '2024-02-03', '2024-02-03 04:05:06.789012', 3.14, 'hi', ['a', 'b'], tuple(5)),
        (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, [], tuple(NULL))
        """)
    result = node.query(f"SELECT * FROM {bq('writable')} ORDER BY id FORMAT TSV")
    expected = (
        "1\trow1\t1.25\ttrue\t2024-02-03\t2024-02-03 04:05:06.789012\t3.14\thi\t['a','b']\t(5)\n"
        "2\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t[]\t(NULL)\n"
    )
    assert result == expected

    # NaN, Infinity and binary values survive the roundtrip.
    node.query(
        f"INSERT INTO FUNCTION {bq('writable')} (id, fl, bin) VALUES (3, nan, 'a\\0b'), (4, inf, ''), (5, -inf, NULL)"
    )
    assert (
        node.query(
            f"SELECT id, fl, hex(bin) FROM {bq('writable')} WHERE id >= 3 ORDER BY id"
        )
        == "3\tnan\t610062\n4\tinf\t\n5\t-inf\t\\N\n"
    )

    # Large inserts are sent in batches of 500 rows.
    mock_reset()
    node.query(f"""
        INSERT INTO FUNCTION {bq('writable')}
        SELECT
            number, toString(number), NULL, NULL, NULL, NULL, NULL, NULL,
            [toString(number)], tuple(number)
        FROM numbers(1200)
        """)
    assert [r["rows"] for r in mock_stats()["insert_requests"]] == [500, 500, 200]
    assert (
        node.query(f"SELECT count(), sum(id) FROM {bq('writable')}")
        == f"1200\t{sum(range(1200))}\n"
    )


def test_insert_errors():
    mock_reset()
    mock_ctl("/__fail_inserts__")
    error = node.query_and_get_error(
        f"INSERT INTO FUNCTION {bq('writable')} (id, ts) VALUES (1, '2024-02-03 04:05:06')"
    )
    assert "BigQuery rejected 1 of 1 rows" in error
    assert "simulated failure" in error
    mock_reset()
    node.query(
        f"INSERT INTO FUNCTION {bq('writable')} (id, ts) VALUES (2, '2024-02-03 04:05:06')"
    )
    assert node.query(f"SELECT count() FROM {bq('writable')}") == "1\n"


def test_service_account_auth():
    key = service_account_key()
    assert (
        node.query(
            f"SELECT count() FROM {bq('test_paging', creds='service_account_key = ' + sql_str(key))}"
        )
        == "10\n"
    )

    bad_key = service_account_key(client_email="intruder@example.com")
    error = node.query_and_get_error(
        f"SELECT count() FROM {bq('test_paging', creds='service_account_key = ' + sql_str(bad_key))}"
    )
    assert "Failed to obtain GCP access token" in error


def test_refresh_token_auth():
    creds = (
        "client_id = 'test-client-id.apps.googleusercontent.com', "
        "client_secret = 'test-client-secret', "
        "refresh_token = 'test-refresh-token', "
        f"token_url = '{BASE_URL}/token'"
    )
    assert node.query(f"SELECT count() FROM {bq('test_paging', creds=creds)}") == "10\n"

    error = node.query_and_get_error(
        f"SELECT count() FROM {bq('test_paging', creds=creds.replace('test-client-secret', 'wrong'))}"
    )
    assert "Failed to obtain GCP access token" in error


def test_named_collection():
    assert node.query("SELECT count() FROM bigquery(bq_mock)") == "3\n"
    assert (
        node.query("SELECT count() FROM bigquery(bq_mock, table = 'test_paging')")
        == "10\n"
    )


def test_table_engine():
    node.query("DROP TABLE IF EXISTS bq_engine")
    node.query(
        f"CREATE TABLE bq_engine ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    create = node.query("SHOW CREATE TABLE bq_engine")
    assert ACCESS_TOKEN not in create
    assert "[HIDDEN]" in create
    # The structure was inferred at CREATE time.
    assert "`i` Int64" in create
    assert node.query("SELECT count() FROM bq_engine") == "10\n"
    node.query("DROP TABLE bq_engine")

    # Declaring a subset of columns with correct types works, wrong types are rejected.
    node.query(
        f"CREATE TABLE bq_engine (s Nullable(String)) ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert node.query("SELECT s FROM bq_engine ORDER BY s LIMIT 1") == "value0\n"
    node.query("DROP TABLE bq_engine")

    node.query(
        f"CREATE TABLE bq_engine (i Int32) ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    error = node.query_and_get_error("SELECT * FROM bq_engine")
    assert "declared as Int32" in error and "maps it to Int64" in error
    node.query("DROP TABLE bq_engine")

    # Writing through the engine.
    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_writable")
    node.query(
        f"CREATE TABLE bq_writable ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'writable', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    node.query("INSERT INTO bq_writable (id, name) VALUES (42, 'x')")
    assert node.query("SELECT id, name FROM bq_writable") == "42\tx\n"
    node.query("DROP TABLE bq_writable")


def test_secret_masking_in_query_log():
    query_id = "bigquery-masking-test"
    node.query(
        f"SELECT count() FROM bigquery('{PROJECT}', '{DATASET}', 'test_paging', '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    node.query("SYSTEM FLUSH LOGS query_log")
    logged = node.query(
        f"SELECT query FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    )
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "[HIDDEN]" in logged


def test_errors():
    error = node.query_and_get_error(f"SELECT * FROM {bq('no_such_table')}")
    assert "Not found: Table" in error

    error = node.query_and_get_error(f"SELECT * FROM {bq('a_view')}")
    assert "cannot be read directly" in error

    wrong_token = bq("test_paging", creds="access_token = 'wrong-token'")
    error = node.query_and_get_error(f"SELECT * FROM {wrong_token}")
    assert "invalid authentication credentials" in error

    no_creds = bq("test_paging", creds="")
    error = node.query_and_get_error(f"SELECT * FROM {no_creds}")
    assert "No credentials specified for BigQuery" in error

    two_methods = bq(
        "test_paging", creds="access_token = 'a', service_account_key = 'b'"
    )
    error = node.query_and_get_error(f"SELECT * FROM {two_methods}")
    assert "Multiple credential methods" in error

    unknown_arg = bq("test_paging", creds="unknown_arg = 'x'")
    error = node.query_and_get_error(f"SELECT * FROM {unknown_arg}")
    assert "Unknown BigQuery argument" in error
