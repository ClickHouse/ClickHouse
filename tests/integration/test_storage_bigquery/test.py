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
# A node with the server-wide `disable_insertion_and_mutation` setting enabled, used to check that
# BigQuery inserts are exempt from it (like other external database engines). It reaches the mock
# server, which runs inside the "node" container, over the cluster network.
read_only_node = cluster.add_instance(
    "read_only_node",
    main_configs=["configs/named_collections.xml", "configs/read_only.xml"],
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
        "dt": "Nullable(DateTime64(6, \\'UTC\\'))",
        "ts": "Nullable(DateTime64(6, \\'UTC\\'))",
        "num": "Nullable(Decimal(38, 9))",
        "bignum": "Nullable(Decimal(76, 38))",
        "num_p": "Nullable(Decimal(10, 2))",
        "geo": "Nullable(String)",
        "j": "Nullable(String)",
        "arr": "Array(Nullable(Int64))",
        "rec": "Nullable(Tuple(x Nullable(Int64), y String, tags Array(Nullable(String))))",
        "recs": "Array(Nullable(Tuple(k Int64, val Nullable(String))))",
    }


def test_array_with_null_element():
    # A REPEATED field is inferred as Array(Nullable(...)) so that NULL elements
    # returned by tabledata.list are preserved instead of being coerced to a default.
    result = node.query(f"DESCRIBE TABLE {bq('test_arr_nulls')}")
    name_to_type = dict(line.split("\t")[:2] for line in result.strip().split("\n"))
    assert name_to_type == {
        "i": "Int64",
        "arr": "Array(Nullable(Int64))",
        "tags": "Array(Nullable(String))",
    }

    result = node.query(f"SELECT arr, tags FROM {bq('test_arr_nulls')} FORMAT TSV")
    assert result == "[1,NULL,2]\t['a',NULL]\n"


def test_select_all_types():
    result = node.query(f"SELECT * FROM {bq('test_types')} ORDER BY i FORMAT TSV")
    expected = (
        "1\t1.5\thello\tbinary-data\ttrue\t2024-01-02\t03:04:05.123456\t"
        "2024-01-02 03:04:05.123456\t2024-01-02 03:04:05.123456\t12345.123456789\t"
        '1234567890.12345678901234567890123456789012345678\t12345678.99\tPOINT(1 2)\t{"a":1}\t'
        "[1,2,3]\t(7,'seven',['t1','t2'])\t[(1,'one'),(2,NULL)]\n"
        "2\tinf\t\t\\N\tfalse\t\\N\t\\N\t\\N\t\\N\t-0.000000001\t\\N\t\\N\t\\N\t\\N\t"
        "[]\t\\N\t[]\n"
        "3\tnan\t\\N\tпривет\t\\N\t1970-01-01\t23:59:59.000000\t2299-12-31 23:59:59.000000\t"
        "1970-01-01 00:00:00.000000\t\\N\t"
        "-99999999999999999999999999999999999999.99999999999999999999999999999999999999\t0.01\t\\N\t"
        "[1,2,3]\t[-1]\t(NULL,'y-only',[])\t[]\n"
    )
    assert result == expected


def test_nullable_record_opt_in():
    # A BigQuery NULLABLE RECORD is inferred as Nullable(Tuple(...)) so whole-record NULLs round-trip
    # (see test_select_all_types, where `rec` for i=2 comes back as NULL). Declaring such a column
    # with the BigQuery table engine persists a Nullable(Tuple) column, which requires the
    # enable_nullable_tuple_type setting, as for any Nullable(Tuple) column. The engine accepts either
    # the exact Nullable(Tuple(...)) (or Array(Nullable(Tuple(...)))) or a plain Tuple(...).
    settings = {"enable_nullable_tuple_type": 1}

    # Reading a NULL RECORD losslessly.
    node.query("DROP TABLE IF EXISTS bq_nullable_rec")
    node.query(
        "CREATE TABLE bq_nullable_rec "
        "(i Int64, rec Nullable(Tuple(x Nullable(Int64), y String, tags Array(Nullable(String))))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_types', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        settings=settings,
    )
    assert (
        node.query(
            "SELECT i, rec FROM bq_nullable_rec ORDER BY i FORMAT TSV",
            settings=settings,
        )
        == "1\t(7,'seven',['t1','t2'])\n2\t\\N\n3\t(NULL,'y-only',[])\n"
    )
    node.query("DROP TABLE bq_nullable_rec")

    # Array(Nullable(Tuple(...))) is likewise accepted for a REPEATED RECORD field.
    node.query("DROP TABLE IF EXISTS bq_nullable_recs")
    node.query(
        "CREATE TABLE bq_nullable_recs "
        "(i Int64, recs Array(Nullable(Tuple(k Int64, val Nullable(String))))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_types', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        settings=settings,
    )
    assert (
        node.query(
            "SELECT i, recs FROM bq_nullable_recs ORDER BY i FORMAT TSV",
            settings=settings,
        )
        == "1\t[(1,'one'),(2,NULL)]\n2\t[]\n3\t[]\n"
    )
    node.query("DROP TABLE bq_nullable_recs")

    # Writing a NULL Nullable(Tuple) record: it is sent as a JSON null and round-trips as NULL.
    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_nullable_write")
    node.query(
        "CREATE TABLE bq_nullable_write (id Int64, meta Nullable(Tuple(a Nullable(Int64)))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'writable', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        settings=settings,
    )
    node.query(
        "INSERT INTO bq_nullable_write VALUES (1, NULL), (2, tuple(7))",
        settings=settings,
    )
    assert (
        node.query(
            "SELECT id, meta FROM bq_nullable_write ORDER BY id FORMAT TSV",
            settings=settings,
        )
        == "1\t\\N\n2\t(7)\n"
    )
    node.query("DROP TABLE bq_nullable_write")

    # The opt-in only strips Nullable placed directly around a Tuple: a nullability difference on a
    # non-record field (here the REQUIRED `y`) is still a type mismatch and is rejected.
    node.query("DROP TABLE IF EXISTS bq_wrong_null")
    node.query(
        "CREATE TABLE bq_wrong_null "
        "(i Int64, rec Nullable(Tuple(x Nullable(Int64), y Nullable(String), tags Array(Nullable(String))))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_types', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        settings=settings,
    )
    error = node.query_and_get_error("SELECT rec FROM bq_wrong_null", settings=settings)
    assert "declared as" in error and "maps it to" in error
    node.query("DROP TABLE bq_wrong_null")


def test_nested_record_nullability_not_moved():
    # The `test_nested_rec` table has a NULLABLE outer RECORD (`parent`) whose inner RECORD (`child`)
    # is REQUIRED, so it is inferred as Nullable(Tuple(child Tuple(x Int64))). The Nullable-around-Tuple
    # relaxation must apply only at the same record node: a declaration that moves the Nullable to the
    # inner record encodes different NULL states and must be rejected, even though both trees have the
    # same shape once nullable-tuple wrappers are ignored.
    settings = {"enable_nullable_tuple_type": 1}

    # Moved nullability: outer plain, inner Nullable - rejected.
    node.query("DROP TABLE IF EXISTS bq_nested_moved")
    node.query(
        "CREATE TABLE bq_nested_moved "
        "(i Int64, parent Tuple(child Nullable(Tuple(x Int64)))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_nested_rec', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        settings=settings,
    )
    error = node.query_and_get_error(
        "SELECT parent FROM bq_nested_moved", settings=settings
    )
    assert "declared as" in error and "maps it to" in error
    node.query("DROP TABLE bq_nested_moved")

    # The exact inferred type is accepted and NULL outer records round-trip losslessly.
    node.query("DROP TABLE IF EXISTS bq_nested_exact")
    node.query(
        "CREATE TABLE bq_nested_exact "
        "(i Int64, parent Nullable(Tuple(child Tuple(x Int64)))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_nested_rec', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        settings=settings,
    )
    assert (
        node.query(
            "SELECT i, parent FROM bq_nested_exact ORDER BY i FORMAT TSV",
            settings=settings,
        )
        == "1\t((5))\n2\t\\N\n"
    )
    node.query("DROP TABLE bq_nested_exact")

    # Dropping the outer record's Nullable (a plain Tuple at the same node) stays accepted and needs no
    # setting; a whole-record NULL then coerces to a default tuple.
    node.query("DROP TABLE IF EXISTS bq_nested_drop")
    node.query(
        "CREATE TABLE bq_nested_drop "
        "(i Int64, parent Tuple(child Tuple(x Int64))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_nested_rec', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert (
        node.query("SELECT i, parent FROM bq_nested_drop ORDER BY i FORMAT TSV")
        == "1\t((5))\n2\t((0))\n"
    )
    node.query("DROP TABLE bq_nested_drop")


def test_count_and_paging():
    mock_reset()
    assert node.query(f"SELECT count(), sum(i) FROM {bq('test_paging')}") == "10\t45\n"
    # The mock caps pages at 4 rows, so reading 10 rows takes 3 requests.
    assert len(mock_stats()["data_requests"]) == 3


def test_selected_fields():
    mock_reset()
    # The columns are requested in reverse order on purpose: the response returns them
    # in the schema order, and ClickHouse must reorder them for the query.
    result = node.query(f"SELECT s, i FROM {bq('test_types')} WHERE i = 1")
    assert result == "hello\t1\n"

    requests = mock_stats()["data_requests"]
    assert all(r["params"]["selectedFields"] == "i,s" for r in requests)

    # Even when all columns are selected, the explicit field list is sent so that execution stays
    # pinned to the analyzed schema snapshot (an empty selectedFields would mean "all current
    # columns", which could differ from the snapshot if the remote table gains a column).
    mock_reset()
    result = node.query(f"SELECT s, i FROM {bq('test_paging')} WHERE i < 2 ORDER BY i")
    assert result == "value0\t0\nvalue1\t1\n"
    requests = mock_stats()["data_requests"]
    assert all(r["params"]["selectedFields"] == "i,s" for r in requests)

    mock_reset()
    result = node.query(f"SELECT sum(i) FROM {bq('test_paging')}")
    assert result == "45\n"
    requests = mock_stats()["data_requests"]
    assert all(r["params"]["selectedFields"] == "i" for r in requests)


def test_no_referenced_columns():
    # A query that references no physical columns at all (a bare count(), a constant
    # projection): the planner requests the smallest column from the storage, so the
    # read path always parses at least one field and row counts stay correct.
    for analyzer in (0, 1):
        settings = {"enable_analyzer": analyzer}
        mock_reset()
        assert (
            node.query(f"SELECT count() FROM {bq('test_paging')}", settings=settings)
            == "10\n"
        )
        requests = mock_stats()["data_requests"]
        # The mock caps pages at 4 rows, so counting 10 rows takes 3 requests.
        assert len(requests) == 3
        assert all(r["params"]["selectedFields"] == "i" for r in requests)

        assert (
            node.query(f"SELECT 1 FROM {bq('test_paging')}", settings=settings)
            == "1\n" * 10
        )


# Must match bigquery_mock_server.py::wide_column_name (the mock's `test_wide` table has 40 such columns
# plus a leading `i`, wide enough that a full column list overflows the request URL length limit).
def wide_column_name(i):
    return "wide_column_" + str(i).zfill(4) + "_" + "x" * 280


def test_wide_select_all_rejected():
    mock_reset()
    # `SELECT *` on a very wide table would make the comma-separated `selectedFields` list longer than the
    # request URL can hold. `selectedFields` is the only way to pin the read to the analyzed schema
    # snapshot (an empty list returns all *current* columns, whose positional `tabledata.list` response
    # could be misaligned by a concurrent schema change without tripping the per-row cell-count check), so
    # rather than read unpinned the query is rejected: the user must project fewer columns.
    error = node.query_and_get_error(f"SELECT * FROM {bq('test_wide')}")
    assert "too long" in error
    # No unpinned "all current columns" request is ever sent.
    assert mock_stats()["data_requests"] == []


def test_wide_projection_rejected():
    mock_reset()
    # A wide *projection* cannot omit `selectedFields` (an empty list would read all columns, not the
    # projected subset), so when the explicit list is too long to fit the request URL the query is
    # rejected with a clear error instead of producing an oversized GET.
    columns = ", ".join(f"`{wide_column_name(i)}`" for i in range(30))
    error = node.query_and_get_error(f"SELECT {columns} FROM {bq('test_wide')}")
    assert "too long" in error
    # The oversized request is never sent.
    assert mock_stats()["data_requests"] == []


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


def test_insert_id_present():
    # Every streamed row carries a stable insertId of the form "<query id>-<row ordinal>", so that
    # BigQuery can best-effort deduplicate retried rows.
    mock_reset()
    node.query(
        f"INSERT INTO FUNCTION {bq('writable')} (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    )
    ids = [iid for r in mock_stats()["insert_requests"] for iid in r["insert_ids"]]
    assert len(ids) == 3
    assert all(iid for iid in ids)
    # A single common query-id prefix plus a monotonic per-row ordinal.
    assert len({iid.rsplit("-", 1)[0] for iid in ids}) == 1
    assert [iid.rsplit("-", 1)[1] for iid in ids] == ["0", "1", "2"]


def test_insert_long_query_id():
    # BigQuery rejects insertId values longer than 128 characters, but the ClickHouse query_id used as
    # the dedup prefix is user-controllable. A long query_id must be bounded (hashed) so every insertId
    # stays valid. The mock rejects overlong insertIds, so this insert fails unless the prefix is bounded.
    # (INSERT ... SELECT is used so the client query_id reaches the sink, unlike INSERT ... VALUES.)
    mock_reset()
    long_query_id = "q" * 200
    node.query(
        f"INSERT INTO FUNCTION {bq('writable')} (id, name) "
        f"SELECT number, toString(number) FROM numbers(3)",
        query_id=long_query_id,
        settings={"max_threads": 1, "max_insert_threads": 1},
    )
    ids = [iid for r in mock_stats()["insert_requests"] for iid in r["insert_ids"]]
    assert len(ids) == 3
    assert all(iid and len(iid) <= 128 for iid in ids)
    # Still a single stable prefix plus per-row ordinals, so insertId deduplication keeps working.
    assert len({iid.rsplit("-", 1)[0] for iid in ids}) == 1
    assert [iid.rsplit("-", 1)[1] for iid in ids] == ["0", "1", "2"]
    assert node.query(f"SELECT count() FROM {bq('writable')}") == "3\n"


def test_insert_flushes_by_bytes():
    # BigQuery's streaming API rejects insertAll requests larger than 10 MB. A batch of wide rows must
    # be split by serialized size (not only by the 500-row cap) so each request stays under the limit.
    # The mock rejects oversized requests, so this insert fails unless the sink flushes by bytes.
    mock_reset()
    node.query(
        f"INSERT INTO FUNCTION {bq('writable')} (id, name) "
        f"SELECT number, repeat('x', 700000) FROM numbers(16)",
        settings={"max_threads": 1, "max_insert_threads": 1},
    )
    requests = mock_stats()["insert_requests"]
    # ~11 MB total across 16 rows (well below the 500-row cap), split into more than one request,
    # each below BigQuery's 10 MB limit.
    assert len(requests) > 1
    assert all(r["body_bytes"] <= 10 * 1024 * 1024 for r in requests)
    assert sum(r["rows"] for r in requests) == 16
    assert node.query(f"SELECT count() FROM {bq('writable')}") == "16\n"


def test_insert_large_single_row():
    # A single row close to (but under) BigQuery's 10 MB request limit must be accepted: the sink budgets
    # against the full serialized request body (envelope + row + commas), not a blanket sub-limit margin, so
    # a ~9.5 MB row goes through in its own request. The previous 9 MiB margin would have rejected it
    # locally even though BigQuery accepts it.
    mock_reset()
    # `repeat` caps the repeat count at 1,000,000, so repeat a 10-byte unit to reach ~9.5 MB.
    node.query(
        f"INSERT INTO FUNCTION {bq('writable')} (id, name) SELECT 1, repeat('xxxxxxxxxx', 950000)",
        settings={"max_threads": 1, "max_insert_threads": 1},
    )
    requests = mock_stats()["insert_requests"]
    assert len(requests) == 1
    assert requests[0]["rows"] == 1
    assert 9500000 < requests[0]["body_bytes"] <= 10 * 1024 * 1024
    assert node.query(f"SELECT count() FROM {bq('writable')}") == "1\n"


def test_insert_allowed_when_mutations_disabled():
    # BigQuery is a write-capable external database, so INSERTs must be exempt from the server-wide
    # `disable_insertion_and_mutation` setting, like MySQL / PostgreSQL / etc.
    mock_reset()
    # The mock server runs inside the "node" container; read_only_node reaches it over the network.
    args = (
        f"'{PROJECT}', '{DATASET}', 'writable', "
        f"base_url = 'http://node:{MOCK_PORT}', access_token = '{ACCESS_TOKEN}'"
    )

    # Sanity check that the setting is actually enabled on this node: a local insert is rejected.
    read_only_node.query(
        "CREATE TABLE IF NOT EXISTS local_t (x UInt64) ENGINE = MergeTree ORDER BY x"
    )
    error = read_only_node.query_and_get_error("INSERT INTO local_t VALUES (1)")
    assert "prohibited" in error.lower()

    # The BigQuery insert, however, is allowed and reaches the remote table.
    read_only_node.query(
        f"INSERT INTO FUNCTION bigquery({args}) (id, name) VALUES (1, 'a')"
    )
    assert read_only_node.query(f"SELECT count() FROM bigquery({args})") == "1\n"


def test_insert_partial_commit_dedup():
    # Streaming inserts are not atomic across batches: a failure in a later batch leaves the earlier
    # batches committed. Re-running the same INSERT with the same query id must not duplicate the
    # already-committed prefix, because the stable insertIds let BigQuery deduplicate it.
    mock_reset()
    insert = f"""
        INSERT INTO FUNCTION {bq('writable')}
        SELECT
            number, toString(number), NULL, NULL, NULL, NULL, NULL, NULL,
            [toString(number)], tuple(number)
        FROM numbers(1200)
        """
    # A single ordered stream so the row -> insertId mapping is identical on both runs.
    settings = {"max_threads": 1, "max_insert_threads": 1}

    # Reject any request once 1000 rows are already committed: the first two 500-row batches land,
    # the third is rejected and the whole INSERT fails - but 1000 rows stay in BigQuery.
    mock_ctl("/__fail_inserts_after__?rows=1000")
    error = node.query_and_get_error(
        insert, query_id="bq_partial_commit", settings=settings
    )
    assert "BigQuery rejected" in error
    assert node.query(f"SELECT count() FROM {bq('writable')}") == "1000\n"

    # Retry the identical INSERT with the same query id. The committed prefix is deduplicated by
    # insertId, so exactly 1200 distinct rows end up in the table instead of 2200.
    mock_ctl("/__fail_inserts_after__")
    node.query(insert, query_id="bq_partial_commit", settings=settings)
    assert (
        node.query(f"SELECT count(), sum(id) FROM {bq('writable')}")
        == f"1200\t{sum(range(1200))}\n"
    )


def test_insert_large_integer():
    # INT64 values are sent to insertAll as decimal strings: BigQuery parses JSON numbers as
    # IEEE-754 doubles, so a value outside [-2^53 + 1, 2^53 - 1] sent as a number would be
    # corrupted. Assert the wire value is a JSON string (not a number), including a nested RECORD
    # field that reuses the same Integer serialization, and that large values round-trip exactly.
    mock_reset()
    big = 9223372036854775807  # 2^63 - 1, far above 2^53 - 1
    small = -9223372036854775807  # -(2^63 - 1)
    node.query(
        f"INSERT INTO FUNCTION {bq('writable')} (id, name, meta) VALUES "
        f"({big}, 'a', tuple({big})), ({small}, 'b', tuple({small}))"
    )

    raw = [r for req in mock_stats()["insert_requests"] for r in req["raw_rows"]]
    assert len(raw) == 2
    for r in raw:
        assert isinstance(r["id"], str), r
        assert isinstance(r["meta"]["a"], str), r
    assert {r["id"] for r in raw} == {str(big), str(small)}

    assert (
        node.query(f"SELECT id, meta FROM {bq('writable')} ORDER BY id FORMAT TSV")
        == f"{small}\t({small})\n{big}\t({big})\n"
    )


def test_range_read_and_write():
    # RANGE is exposed as a read-only String. Reading returns the formatted range text; an INSERT
    # into a RANGE column is rejected, because insertAll needs a structured {start, end} payload
    # that cannot be reconstructed from the String mapping.
    mock_reset()
    assert (
        node.query(f"SELECT i, r FROM {bq('test_range')} ORDER BY i FORMAT TSV")
        == "1\t[2020-01-01, 2020-12-31)\n2\t[2021-01-01, UNBOUNDED)\n"
    )

    error = node.query_and_get_error(
        f"INSERT INTO FUNCTION {bq('test_range')} (i, r) VALUES (3, '[2022-01-01, 2022-12-31)')"
    )
    assert "RANGE" in error and "not supported" in error
    # The write is rejected before any row is streamed to insertAll.
    assert mock_stats()["insert_requests"] == []


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


def test_named_collection_dependency():
    # A permanent BigQuery table created from a named collection registers a dependency, so the
    # named collection cannot be dropped while the table still uses it.
    node.query("DROP TABLE IF EXISTS bq_nc_dep")
    node.query("DROP NAMED COLLECTION IF EXISTS bq_dep")
    node.query(
        "CREATE NAMED COLLECTION bq_dep AS "
        f"project = '{PROJECT}', dataset = '{DATASET}', table = 'test_paging', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}'"
    )
    node.query("CREATE TABLE bq_nc_dep ENGINE = BigQuery(bq_dep)")

    error = node.query_and_get_error("DROP NAMED COLLECTION bq_dep")
    assert "is used by tables" in error
    assert "bq_nc_dep" in error

    # Once the table is dropped, the dependency is gone and the collection can be dropped.
    node.query("DROP TABLE bq_nc_dep")
    node.query("DROP NAMED COLLECTION bq_dep")


def test_table_function_reuses_schema_snapshot():
    # The table function fetches the schema once during analysis and hands that snapshot (and the
    # token provider) to the storage, so a single query does not issue a second tables.get at
    # execution time (and does not mint a second OAuth token).
    mock_reset()
    assert (
        node.query(f"SELECT * FROM {bq('test_paging')} ORDER BY i LIMIT 1 FORMAT TSV")
        == "0\tvalue0\n"
    )
    assert len(mock_stats()["schema_requests"]) == 1

    # The same holds for a refreshable credential: the token is minted once and reused for both
    # schema inference and execution.
    creds = (
        "client_id = 'test-client-id.apps.googleusercontent.com', "
        "client_secret = 'test-client-secret', "
        "refresh_token = 'test-refresh-token', "
        f"token_url = '{BASE_URL}/token'"
    )
    mock_reset()
    assert node.query(f"SELECT count() FROM {bq('test_paging', creds=creds)}") == "10\n"
    assert len(mock_stats()["schema_requests"]) == 1
    assert len(mock_stats()["token_requests"]) == 1


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

    # Writing through the engine. The `writable` table has a NULLABLE RECORD (`meta`), inferred as
    # Nullable(Tuple(...)), so creating the table with an inferred structure needs the setting -
    # without it, the CREATE is rejected instead of silently persisting a Nullable(Tuple) column.
    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_writable")
    error = node.query_and_get_error(
        f"CREATE TABLE bq_writable ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'writable', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert "enable_nullable_tuple_type" in error

    node.query(
        f"CREATE TABLE bq_writable ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'writable', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        settings={"enable_nullable_tuple_type": 1},
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


def test_secret_masking_constant_expression():
    # The positional access token does not have to be a string literal: any constant expression
    # (e.g. `concat(...)`) is folded during parsing. Masking must not depend on the literal form,
    # otherwise the token pieces leak into `system.query_log` verbatim.
    token_expr = "concat('test-static', '-token')"
    query_id = "bigquery-masking-concat-test"
    node.query(
        f"SELECT count() FROM bigquery('{PROJECT}', '{DATASET}', 'test_paging', {token_expr}, base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    node.query("SYSTEM FLUSH LOGS query_log")
    logged = node.query(
        f"SELECT query FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    )
    assert logged != ""
    assert "test-static" not in logged
    assert "-token" not in logged
    assert "[HIDDEN]" in logged

    # The same for the engine form: the CREATE query is masked in the query log, and the token
    # (folded to a literal during argument evaluation) is masked in SHOW CREATE TABLE.
    node.query("DROP TABLE IF EXISTS bq_engine_concat")
    create_query_id = "bigquery-masking-concat-create"
    node.query(
        f"CREATE TABLE bq_engine_concat ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"{token_expr}, base_url = '{BASE_URL}')",
        query_id=create_query_id,
    )
    create = node.query("SHOW CREATE TABLE bq_engine_concat")
    assert ACCESS_TOKEN not in create
    assert "test-static" not in create
    assert "[HIDDEN]" in create
    node.query("SYSTEM FLUSH LOGS query_log")
    logged = node.query(
        f"SELECT query FROM system.query_log WHERE query_id = '{create_query_id}' AND type = 'QueryFinish'"
    )
    assert logged != ""
    assert "test-static" not in logged
    assert "-token" not in logged
    assert "[HIDDEN]" in logged
    node.query("DROP TABLE bq_engine_concat")

    # `key = value` arguments can be interleaved with positional ones; the 4th *positional*
    # argument is masked wherever it appears, and non-secret arguments stay visible.
    query_id = "bigquery-masking-interleaved-test"
    node.query(
        f"SELECT count() FROM bigquery('{PROJECT}', base_url = '{BASE_URL}', '{DATASET}', 'test_paging', '{ACCESS_TOKEN}')",
        query_id=query_id,
    )
    node.query("SYSTEM FLUSH LOGS query_log")
    logged = node.query(
        f"SELECT query FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    )
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "[HIDDEN]" in logged
    assert BASE_URL in logged


def test_unsupported_table_types():
    # Only native tables can be read; views, materialized views and external tables are rejected
    # up front with a clear error, matching the documented contract.
    for table, type_ in [("a_view", "VIEW"), ("a_matview", "MATERIALIZED_VIEW")]:
        error = node.query_and_get_error(f"SELECT * FROM {bq(table)}")
        assert "cannot be read directly" in error
        assert type_ in error


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
