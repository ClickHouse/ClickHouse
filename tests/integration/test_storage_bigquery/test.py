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
        # GEOGRAPHY is mapped to `Geometry`, which is a `Variant` and represents a NULL by itself, so a
        # NULLABLE GEOGRAPHY field is not wrapped in `Nullable`.
        "geo": "Geometry",
        "j": "Nullable(String)",
        "arr": "Array(Int64)",
        "rec": "Nullable(Tuple(x Nullable(Int64), y String, tags Array(String)))",
        # DESCRIBE renders a bare (non-Nullable) named Tuple inside an Array on multiple lines, escaped
        # as `\n` in the TabSeparated output (a Nullable(Tuple(...)) prints on a single line, see `rec`).
        "recs": "Array(Tuple(\\n    k Int64,\\n    val Nullable(String)))",
    }


def test_array_null_element_rejected():
    # A BigQuery ARRAY cannot store NULL elements (ARRAY<T> is equivalent to ARRAY<T NOT NULL>), so a
    # REPEATED field is inferred with a non-Nullable element type: DESCRIBE / SHOW CREATE do not advertise
    # element nullability the backend cannot persist, and a natural Array(T) can be declared explicitly.
    result = node.query(f"DESCRIBE TABLE {bq('test_arr_nulls')}")
    name_to_type = dict(line.split("\t")[:2] for line in result.strip().split("\n"))
    assert name_to_type == {
        "i": "Int64",
        "arr": "Array(Int64)",
        "tags": "Array(String)",
    }

    # The mock table serves a {"v": null} array element (which a real BigQuery table cannot produce). Such
    # a payload is malformed input and is rejected rather than being coerced into a default element.
    error = node.query_and_get_error(
        f"SELECT arr, tags FROM {bq('test_arr_nulls')} FORMAT TSV"
    )
    assert "cannot contain NULL elements" in error


def test_select_all_types():
    result = node.query(f"SELECT * FROM {bq('test_types')} ORDER BY i FORMAT TSV")
    expected = (
        "1\t1.5\thello\tbinary-data\ttrue\t2024-01-02\t03:04:05.123456\t"
        "2024-01-02 03:04:05.123456\t2024-01-02 03:04:05.123456\t12345.123456789\t"
        '1234567890.12345678901234567890123456789012345678\t12345678.99\t(1,2)\t{"a":1}\t'
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
    # the exact Nullable(Tuple(...)) or a plain Tuple(...).
    settings = {"enable_nullable_tuple_type": 1}

    # Reading a NULL RECORD losslessly.
    node.query("DROP TABLE IF EXISTS bq_nullable_rec")
    node.query(
        "CREATE TABLE bq_nullable_rec "
        "(i Int64, rec Nullable(Tuple(x Nullable(Int64), y String, tags Array(String)))) "
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

    # A REPEATED RECORD field is inferred as Array(Tuple(...)) (a BigQuery array cannot contain NULL
    # elements), so it needs no enable_nullable_tuple_type setting. A NULL field inside a record element
    # still round-trips through the field's own Nullable type.
    node.query("DROP TABLE IF EXISTS bq_nullable_recs")
    node.query(
        "CREATE TABLE bq_nullable_recs "
        "(i Int64, recs Array(Tuple(k Int64, val Nullable(String)))) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_types', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert (
        node.query("SELECT i, recs FROM bq_nullable_recs ORDER BY i FORMAT TSV")
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
        "(i Int64, rec Nullable(Tuple(x Nullable(Int64), y Nullable(String), tags Array(String)))) "
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


def test_limit_reduces_the_read():
    # Predicates cannot be pushed down into `tabledata.list`, but a LIMIT can: the pages are fetched
    # lazily with `maxResults` set to `max_block_size`, and for a trivial LIMIT the planner lowers
    # `max_block_size` to the limit itself, so a single request for exactly that many rows is made.
    mock_reset()
    assert node.query(f"SELECT i FROM {bq('test_paging')} LIMIT 3") == "0\n1\n2\n"
    requests = mock_stats()["data_requests"]
    assert len(requests) == 1
    assert requests[0]["params"]["maxResults"] == "3"

    # A limit above the server's page size (the mock caps pages at 4 rows) stops the pagination as
    # soon as it is satisfied, instead of downloading all 10 rows of the table in 3 requests.
    mock_reset()
    assert (
        node.query(f"SELECT i FROM {bq('test_paging')} LIMIT 6") == "0\n1\n2\n3\n4\n5\n"
    )
    assert len(mock_stats()["data_requests"]) == 2

    # Without a limit, the whole table is read.
    mock_reset()
    assert node.query(f"SELECT count() FROM {bq('test_paging')}") == "10\n"
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


def test_wide_projection_near_threshold_rejected():
    mock_reset()
    # The limit is budgeted against the full encoded `tabledata.list` request URI, not just the raw
    # comma-joined `selectedFields` payload. A 27-column projection of these ~297-byte names makes a raw
    # field list of 27 * 297 + 26 = 8045 bytes, which is under the 8192-byte budget; a naive check on the
    # raw payload alone would let it through. But the full request URI adds the table path, the fixed query
    # parameters and the percent-encoding of each `,` (as `%2C`), so the actual URL exceeds the limit and
    # the read is rejected up front rather than failing with an opaque HTTP error near the threshold.
    columns = ", ".join(f"`{wide_column_name(i)}`" for i in range(27))
    error = node.query_and_get_error(f"SELECT {columns} FROM {bq('test_wide')}")
    assert "too long" in error
    # The oversized request is never sent.
    assert mock_stats()["data_requests"] == []


def test_wide_projection_near_threshold_accepted():
    mock_reset()
    # The up-front guard must reject only reads whose *first-page* request URL does not fit: it reserves
    # no headroom for a `pageToken`, because a single-page read never sends one, and a later page whose
    # real token does not fit is rejected by `BigQueryClient::listTableData` when that page is requested.
    # A 26-column projection of these ~297-byte names makes a request URI just under the 8192-byte limit
    # (one column less than test_wide_projection_near_threshold_rejected), and `test_wide` has a single
    # row, so this read fits in one page and must succeed.
    columns = ", ".join(f"`{wide_column_name(i)}`" for i in range(26))
    assert node.query(f"SELECT {columns} FROM {bq('test_wide')} FORMAT TSV") == (
        "\t".join(["1"] * 26) + "\n"
    )
    # A single, explicitly pinned data request was sent.
    data_requests = mock_stats()["data_requests"]
    assert len(data_requests) == 1
    assert data_requests[0]["params"]["selectedFields"] == ",".join(
        wide_column_name(i) for i in range(26)
    )


def test_wide_page_token_rejected():
    mock_reset()
    # The up-front wide-read guard can only measure the first-page request URL: the `pageToken` BigQuery
    # returns for later pages is opaque and unbounded, and is unknown until a page has been fetched. Make the
    # mock return a very long `pageToken` on the first page of a narrow read: the read starts, gets page 1,
    # and then the second `tabledata.list` request would exceed the request-URL length limit. It must be
    # rejected up front, before being sent, rather than failing remotely with an opaque HTTP error mid-read.
    mock_ctl("/__long_page_token__?len=9000")
    error = node.query_and_get_error(
        f"SELECT s, i FROM {bq('test_paging')} FORMAT Null"
    )
    assert "too long" in error
    # Only the first page is fetched; the oversized second request is rejected before being sent.
    assert len(mock_stats()["data_requests"]) == 1


def test_read_schema_reorder_rejected():
    mock_reset()
    # `selectedFields` pins the *set* of requested columns, but the tabledata.list response is positional
    # and ordered by the table's *current* schema, so if the BigQuery table is replaced between analysis
    # and execution with the same column names in a different order, decoding the positional response into
    # the analyzed order would silently swap type-compatible values. The reader re-fetches the live schema
    # right before the read and rejects the query when the requested columns no longer line up. Here the
    # mock serves the original order (a, b) for the analysis-time schema fetch and the swapped order (b, a)
    # for the read's pre-read re-check; both columns are STRING, so the swap would otherwise be silent.
    mock_ctl("/__swap_schema_after_first_get__?table=test_reorder&i=0&j=1")
    error = node.query_and_get_error(
        f"SELECT a, b FROM {bq('test_reorder')} ORDER BY a FORMAT TSV"
    )
    assert "changed between query analysis and execution" in error
    # The read was rejected before any data request was issued.
    assert mock_stats()["data_requests"] == []


def test_same_clickhouse_type_schema_drift_rejected():
    # Both drift checks compare the BigQuery schema nodes themselves - type, mode, precision and scale,
    # and the RECORD children, recursively - not the ClickHouse types they map to. Several BigQuery types
    # share a ClickHouse type (`STRING` and `BYTES` both map to `String`), while the wire encoding is
    # driven by the BigQuery type, so a drift that keeps the mapped type would otherwise pass the guard
    # and be decoded (or encoded) with the wrong rules: a `BYTES` payload is base64-encoded.
    #
    # A `BigQuery` engine table pins the analyzed snapshot: the schema is inferred once, when the table
    # is created, and cached for the lifetime of the storage. The remote type is changed only afterwards.
    def create_engine_table():
        node.query("DROP TABLE IF EXISTS bq_same_type_drift")
        node.query(
            f"CREATE TABLE bq_same_type_drift ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_drift', "
            f"base_url = '{BASE_URL}', access_token = '{ACCESS_TOKEN}')"
        )

    # A top-level STRING -> BYTES drift on the read path.
    mock_reset()
    create_engine_table()
    mock_ctl("/__retype_schema__?table=test_drift&column=s&type=BYTES")
    error = node.query_and_get_error("SELECT i, s FROM bq_same_type_drift")
    assert "changed between query analysis and execution" in error
    # The read was rejected before any data request was issued.
    assert mock_stats()["data_requests"] == []

    # The same drift inside a RECORD child, which collapses into the same `Tuple(name String)`.
    mock_reset()
    create_engine_table()
    mock_ctl("/__retype_schema__?table=test_drift&column=rec.name&type=BYTES")
    error = node.query_and_get_error("SELECT i, rec FROM bq_same_type_drift")
    assert "changed between query analysis and execution" in error
    assert mock_stats()["data_requests"] == []

    # The write path uses the same comparison, so a RECORD child drift is rejected there as well:
    # `bigQueryJSONValue` would start base64-encoding the child while the local type is still `String`.
    mock_reset()
    create_engine_table()
    mock_ctl("/__retype_schema__?table=test_drift&column=rec.name&type=BYTES")
    error = node.query_and_get_error(
        "INSERT INTO bq_same_type_drift VALUES (2, 's1', tuple('n1'))"
    )
    assert "changed since it was analyzed" in error
    assert mock_stats()["insert_requests"] == []

    # Without drift the same statements work, and the nested STRING round-trips as a string.
    mock_reset()
    create_engine_table()
    node.query("INSERT INTO bq_same_type_drift VALUES (2, 's1', tuple('n1'))")
    assert (
        node.query("SELECT i, s, rec.1 FROM bq_same_type_drift ORDER BY i FORMAT TSV")
        == "1\ts0\tn0\n2\ts1\tn1\n"
    )
    node.query("DROP TABLE bq_same_type_drift")


def test_schema_snapshot_is_re_established_after_reload():
    # The drift checks compare the live schema against the snapshot the query was analyzed with. Table
    # metadata persists the mapped ClickHouse columns, not the BigQuery schema, so `DETACH`/`ATTACH` (and
    # a server restart, which takes the same code path) drops the snapshot and the next read or write
    # re-establishes it from the live schema. A schema change made while the table was detached is
    # therefore adopted rather than rejected - the declared columns are still validated against the live
    # schema, and the rows are decoded with it, so nothing is silently mismapped.
    #
    # `bin` is `BYTES`, whose wire payload is base64-encoded, and retyping it to `STRING` keeps the mapped
    # ClickHouse type `String` - the drift that only `bigQueryFieldsIdentical` can see - while leaving the
    # payload readable, so the change of decoding rules is directly observable.
    settings = {
        "enable_nullable_tuple_type": 1
    }  # `test_types` has a NULLABLE RECORD column.

    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_reload")
    node.query(
        f"CREATE TABLE bq_reload ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_types', "
        f"base_url = '{BASE_URL}', access_token = '{ACCESS_TOKEN}')",
        settings=settings,
    )
    assert node.query("SELECT bin FROM bq_reload LIMIT 1 FORMAT TSV") == "binary-data\n"

    node.query("DETACH TABLE bq_reload")
    mock_reset()
    mock_ctl("/__retype_schema__?table=test_types&column=bin&type=STRING")
    node.query("ATTACH TABLE bq_reload")

    # The first read after the reload takes its snapshot now, so there is nothing older to compare it
    # against: the read is not rejected, and it decodes with the live `STRING` rules, returning the raw
    # base64 text instead of the decoded bytes.
    assert (
        node.query("SELECT bin FROM bq_reload LIMIT 1 FORMAT TSV")
        == "YmluYXJ5LWRhdGE=\n"
    )
    # Exactly one `tables.get`: the snapshot fetch doubles as the live schema, so the drift check does not
    # issue a second request only to compare the schema against itself.
    assert len(mock_stats()["schema_requests"]) == 1

    # Once the snapshot is established, the guarantee is back: a further drift is rejected.
    mock_ctl("/__retype_schema__?table=test_types&column=bin&type=BYTES")
    error = node.query_and_get_error("SELECT bin FROM bq_reload LIMIT 1")
    assert "changed between query analysis and execution" in error
    node.query("DROP TABLE bq_reload")


def test_table_function_snapshot_is_re_established_after_reload():
    # A table created with `CREATE TABLE ... AS bigquery(...)` persists only the mapped ClickHouse
    # columns, exactly like a `BigQuery` engine table. After `DETACH`/`ATTACH` (or a server restart)
    # the nested storage is rebuilt from the cached columns with no schema snapshot, and the first
    # read re-establishes it from the live schema, validating the cached columns against it - the
    # same reload contract as the engine form (see the previous test).
    settings = {
        "enable_nullable_tuple_type": 1
    }  # `test_types` has a NULLABLE RECORD column.

    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_tf_reload")
    node.query(
        f"CREATE TABLE bq_tf_reload AS bigquery('{PROJECT}', '{DATASET}', 'test_types', "
        f"base_url = '{BASE_URL}', access_token = '{ACCESS_TOKEN}')",
        settings=settings,
    )
    assert (
        node.query("SELECT bin FROM bq_tf_reload LIMIT 1 FORMAT TSV") == "binary-data\n"
    )

    node.query("DETACH TABLE bq_tf_reload")
    mock_reset()
    mock_ctl("/__retype_schema__?table=test_types&column=bin&type=STRING")
    node.query("ATTACH TABLE bq_tf_reload")

    # The first read after the reload takes its snapshot from the live schema, so it decodes with the
    # live `STRING` rules (the raw base64 text instead of the decoded bytes), and issues exactly one
    # `tables.get` - the snapshot fetch doubles as the live schema for the drift check.
    assert (
        node.query("SELECT bin FROM bq_tf_reload LIMIT 1 FORMAT TSV")
        == "YmluYXJ5LWRhdGE=\n"
    )
    assert len(mock_stats()["schema_requests"]) == 1

    # Once the snapshot is established, a further drift is rejected again.
    mock_ctl("/__retype_schema__?table=test_types&column=bin&type=BYTES")
    error = node.query_and_get_error("SELECT bin FROM bq_tf_reload LIMIT 1")
    assert "changed between query analysis and execution" in error
    node.query("DROP TABLE bq_tf_reload")


def test_write_schema_drift_rejected():
    # The write path re-fetches the live schema before streaming any row, and rejects the INSERT if the
    # touched columns no longer match the analyzed snapshot. Without that check the rows would be sent
    # against a stale snapshot, and `tabledata.insertAll` would happily store them under the new type
    # whenever the two types share a JSON representation.
    #
    # A `BigQuery` engine table pins the snapshot: the schema is inferred once, when the table is created,
    # and cached for the lifetime of the storage. The remote type is changed only afterwards, so the drift
    # does not depend on how many `tables.get` requests a query happens to issue before execution.
    # `writable` has a NULLABLE RECORD column, which is inferred as Nullable(Tuple(...)).
    settings = {"enable_nullable_tuple_type": 1}

    def create_engine_table(name):
        node.query(f"DROP TABLE IF EXISTS {name}")
        node.query(
            f"CREATE TABLE {name} ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'writable', "
            f"base_url = '{BASE_URL}', access_token = '{ACCESS_TOKEN}')",
            settings=settings,
        )

    mock_reset()
    create_engine_table("bq_drift")
    # STRING and BYTES both map to `String`, so the local column type still matches the live schema and
    # only the snapshot comparison can catch the drift - yet a BYTES column expects base64-encoded data,
    # so the rows would be stored corrupted.
    mock_ctl("/__retype_schema__?table=writable&column=name&type=BYTES")
    error = node.query_and_get_error(
        "INSERT INTO bq_drift (id, name) VALUES (1, 'row1')", settings=settings
    )
    assert "changed since it was analyzed" in error
    # The write was rejected before any row was streamed.
    assert mock_stats()["insert_requests"] == []

    mock_reset()
    create_engine_table("bq_drift")
    # A drift that also changes the ClickHouse type is rejected by the column type check: an analyzed
    # `INTEGER` column is serialized as decimal text, so a live `STRING` column would accept the very
    # same payload and silently store `Int64` values as strings.
    mock_ctl("/__retype_schema__?table=writable&column=id&type=STRING")
    error = node.query_and_get_error(
        "INSERT INTO bq_drift (id, name) VALUES (1, 'row1')", settings=settings
    )
    assert "is declared as Int64" in error
    assert mock_stats()["insert_requests"] == []

    # Without drift the same INSERT succeeds, both through the engine table and the table function.
    mock_reset()
    create_engine_table("bq_drift")
    node.query("INSERT INTO bq_drift (id, name) VALUES (1, 'row1')", settings=settings)
    node.query(f"INSERT INTO FUNCTION {bq('writable')} (id, name) VALUES (2, 'row2')")
    assert (
        node.query(f"SELECT id, name FROM {bq('writable')} ORDER BY id")
        == "1\trow1\n2\trow2\n"
    )
    node.query("DROP TABLE bq_drift")


def test_write_rejected_when_required_field_omitted():
    # A table defined with an explicit subset of the BigQuery columns is readable, but writing is
    # rejected up front when an omitted remote field is REQUIRED and has no defaultValueExpression:
    # the sink would never send that field, and `tabledata.insertAll` rejects every such row, so
    # without the check the INSERT could only fail remotely, batch by batch.
    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_subset")
    node.query(
        "CREATE TABLE bq_subset (s Nullable(String)) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_drift', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    # The subset is readable.
    assert node.query("SELECT s FROM bq_subset") == "s0\n"
    # ...but not writable: `i` (and `rec`) are REQUIRED and omitted from the definition.
    error = node.query_and_get_error("INSERT INTO bq_subset VALUES ('x')")
    assert (
        "is `REQUIRED` without a default value expression but is not present" in error
    )
    # The write was rejected before any request.
    assert mock_stats()["insert_requests"] == []
    node.query("DROP TABLE bq_subset")

    # A subset that omits only NULLABLE and REPEATED fields stays writable.
    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_subset_ok")
    node.query(
        "CREATE TABLE bq_subset_ok (id Int64, name Nullable(String)) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'writable', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    node.query("INSERT INTO bq_subset_ok VALUES (7, 'subset')")
    assert (
        node.query(f"SELECT id, name FROM {bq('writable')} WHERE id = 7")
        == "7\tsubset\n"
    )
    node.query("DROP TABLE bq_subset_ok")


def test_write_allowed_when_omitted_required_field_has_default():
    # An omitted REQUIRED field with a defaultValueExpression does not block writes: BigQuery
    # streaming inserts fill the default in for the omitted column, so the up-front reject applies
    # only to REQUIRED fields without a default.
    mock_reset()
    node.query("DROP TABLE IF EXISTS bq_required_default")
    node.query(
        "CREATE TABLE bq_required_default (id Int64) "
        f"ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_required_default', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    node.query("INSERT INTO bq_required_default VALUES (1)")
    assert len(mock_stats()["insert_requests"]) == 1
    # The mock, like BigQuery, filled the omitted `val` from its default value expression.
    assert (
        node.query(f"SELECT id, val FROM {bq('test_required_default')} WHERE id = 1")
        == "1\tfilled-by-default\n"
    )
    node.query("DROP TABLE bq_required_default")


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
    assert all(r["body_bytes"] <= 10_000_000 for r in requests)
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
    assert 9500000 < requests[0]["body_bytes"] <= 10_000_000
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


def test_insert_partial_commit_within_request_dedup():
    # BigQuery's streaming insertAll can partially succeed within a single request: rows listed in
    # insertErrors are rejected while the others are committed (unlike the all-or-nothing schema
    # mismatch case). Re-running the same INSERT with the same query id must deduplicate the rows that
    # already committed, so only the previously-rejected rows are added instead of the whole batch.
    mock_reset()
    insert = f"""
        INSERT INTO FUNCTION {bq('writable')}
        SELECT
            number, toString(number), NULL, NULL, NULL, NULL, NULL, NULL,
            [toString(number)], tuple(number)
        FROM numbers(6)
        """
    # A single ordered stream so the row -> insertId mapping is identical on both runs.
    settings = {"max_threads": 1, "max_insert_threads": 1}

    # All six rows go in one insertAll request; reject the rows id = 2 and id = 4 per-row. The other
    # four rows of that same request commit, but the INSERT still fails because BigQuery reported errors.
    mock_ctl("/__reject_row_ids__?ids=2,4")
    error = node.query_and_get_error(
        insert, query_id="bq_within_request", settings=settings
    )
    assert "BigQuery rejected" in error
    assert len(mock_stats()["insert_requests"]) == 1
    assert node.query(f"SELECT count() FROM {bq('writable')}") == "4\n"

    # Retry the identical INSERT with the same query id, now accepting every row. The four rows that
    # already committed are deduplicated by insertId, so the table holds all six distinct rows (not ten).
    mock_ctl("/__reject_row_ids__")
    node.query(insert, query_id="bq_within_request", settings=settings)
    assert (
        node.query(f"SELECT count(), sum(id) FROM {bq('writable')}")
        == f"6\t{sum(range(6))}\n"
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


def test_geography_read_and_write():
    # GEOGRAPHY is mapped to the `Geometry` type: the WKT text is parsed into the matching alternative
    # of the `Variant` on read and serialized back to WKT on write. `Variant` holds a NULL by itself, so
    # a NULLABLE GEOGRAPHY field is inferred as `Geometry`, not as `Nullable(Geometry)`.
    mock_reset()
    result = node.query(f"DESCRIBE TABLE {bq('test_geo')}")
    name_to_type = dict(line.split("\t")[:2] for line in result.strip().split("\n"))
    assert name_to_type == {"i": "Int64", "g": "Geometry", "garr": "Array(Geometry)"}

    # Every shape is read into its own variant, and the WKT round-trips through `wkt`.
    assert node.query(
        f"SELECT i, variantType(g), wkt(g) FROM {bq('test_geo')} ORDER BY i FORMAT TSV"
    ) == (
        "1\tPoint\tPOINT(1 2)\n"
        "2\tLineString\tLINESTRING(0 0,1 1,2 3)\n"
        "3\tPolygon\tPOLYGON((0 0,0 1,1 1,1 0,0 0))\n"
        "4\tMultiLineString\tMULTILINESTRING((0 0,1 1),(2 2,3 3))\n"
        "5\tMultiPolygon\tMULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))\n"
        "6\tNone\t\\N\n"
        "7\tMultiPoint\tMULTIPOINT((0 0),(1 1))\n"
    )

    # The write path serializes a `Geometry` value back to WKT, and a NULL stays a NULL.
    # A REPEATED GEOGRAPHY field becomes an Array(Geometry).
    assert (
        node.query(
            f"SELECT arrayMap(x -> wkt(x), garr) FROM {bq('test_geo')} WHERE i = 1 FORMAT TSV"
        )
        == "['POINT(3 4)','LINESTRING(0 0,1 1)']\n"
    )

    node.query(
        f"INSERT INTO FUNCTION {bq('test_geo')} (i, g) "
        "SELECT 8, readWKTPoint('POINT(5 6)')::Geometry"
    )
    node.query(f"INSERT INTO FUNCTION {bq('test_geo')} (i, g) SELECT 9, NULL::Geometry")
    # A MultiPoint round-trips through the write path as well.
    node.query(
        f"INSERT INTO FUNCTION {bq('test_geo')} (i, g) "
        "SELECT 10, readWKTMultiPoint('MULTIPOINT(7 8, 9 10)')::Geometry"
    )
    assert node.query(
        f"SELECT i, variantType(g), wkt(g) FROM {bq('test_geo')} WHERE i > 7 ORDER BY i FORMAT TSV"
    ) == (
        "8\tPoint\tPOINT(5 6)\n"
        "9\tNone\t\\N\n"
        "10\tMultiPoint\tMULTIPOINT((7 8),(9 10))\n"
    )


def test_geography_null_rejected_where_bigquery_forbids_it():
    # `Geometry` is a `Variant` that carries its own NULL, so a NULL value can reach the write path even
    # for a REQUIRED field and for an element of a REPEATED field, where BigQuery accepts no NULL. Such a
    # value is rejected locally instead of being sent as JSON `null` and failing inside `insertAll`.
    mock_reset()
    error = node.query_and_get_error(
        f"INSERT INTO FUNCTION {bq('test_geo_required')} (i, g) SELECT 1, NULL::Geometry"
    )
    assert "REQUIRED" in error and "NULL" in error
    assert mock_stats()["insert_requests"] == []

    error = node.query_and_get_error(
        f"INSERT INTO FUNCTION {bq('test_geo')} (i, garr) "
        "SELECT 11, [readWKTPoint('POINT(1 2)')::Geometry, NULL::Geometry]"
    )
    assert "REPEATED" in error and "NULL" in error
    assert mock_stats()["insert_requests"] == []

    # A non-NULL value of the same shape is accepted.
    node.query(
        f"INSERT INTO FUNCTION {bq('test_geo_required')} (i, g) "
        "SELECT 2, readWKTPoint('POINT(1 2)')::Geometry"
    )
    assert (
        node.query(f"SELECT i, wkt(g) FROM {bq('test_geo_required')} FORMAT TSV")
        == "2\tPOINT(1 2)\n"
    )


def test_geography_unsupported_shape_rejected():
    # A GEOMETRYCOLLECTION has no counterpart in the `Geometry` type, so reading such a value raises an
    # error instead of silently coercing it to a different shape.
    mock_reset()
    error = node.query_and_get_error(
        f"SELECT g FROM {bq('test_geo_collection')} FORMAT TSV"
    )
    assert "GEOMETRYCOLLECTION" in error and "not supported" in error


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


def test_named_collection_duplicate_override():
    # Repeating a `key = value` override on a named collection is rejected the same way the
    # plain-argument form rejects a repeated key, instead of silently applying the last value.
    error = node.query_and_get_error(
        "SELECT count() FROM bigquery(bq_mock, table = 'test_paging', table = 'test_types')"
    )
    assert "more than once" in error

    node.query("DROP TABLE IF EXISTS bq_nc_dup")
    error = node.query_and_get_error(
        "CREATE TABLE bq_nc_dup ENGINE = BigQuery(bq_mock, table = 'test_paging', table = 'test_types')"
    )
    assert "more than once" in error

    # A single override of the same key is still allowed.
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


def test_named_collection_dependency_table_function():
    # A permanent table created from the table function (`CREATE TABLE ... AS bigquery(collection)`)
    # holds the named collection the same way the table engine does: `DROP NAMED COLLECTION` is
    # blocked while the table exists, including after the table is re-attached (the dependency is
    # re-established when the stored definition is loaded).
    node.query("DROP TABLE IF EXISTS bq_nc_tf_dep")
    node.query("DROP NAMED COLLECTION IF EXISTS bq_tf_dep")
    node.query(
        "CREATE NAMED COLLECTION bq_tf_dep AS "
        f"project = '{PROJECT}', dataset = '{DATASET}', table = 'test_paging', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}'"
    )
    node.query("CREATE TABLE bq_nc_tf_dep AS bigquery(bq_tf_dep)")

    error = node.query_and_get_error("DROP NAMED COLLECTION bq_tf_dep")
    assert "is used by tables" in error
    assert "bq_nc_tf_dep" in error

    # The dependency survives a DETACH/ATTACH cycle: attaching the stored
    # `CREATE TABLE ... AS bigquery(...)` definition registers it again.
    node.query("DETACH TABLE bq_nc_tf_dep")
    node.query("ATTACH TABLE bq_nc_tf_dep")
    error = node.query_and_get_error("DROP NAMED COLLECTION bq_tf_dep")
    assert "is used by tables" in error
    assert "bq_nc_tf_dep" in error

    # The transient use of the table function in a query does not register anything, so after the
    # table is dropped the collection can be dropped even though it was just queried.
    node.query("DROP TABLE bq_nc_tf_dep")
    assert node.query("SELECT count() FROM bigquery(bq_tf_dep)") == "10\n"
    node.query("DROP NAMED COLLECTION bq_tf_dep")


def test_table_function_reuses_schema_snapshot():
    # The table function fetches the schema once during analysis and hands that snapshot (and the token
    # provider) to the storage, so the structure is not re-inferred at execution time. The read adds exactly
    # one more tables.get: a fail-close drift check that verifies the live schema still matches the analyzed
    # snapshot before decoding the positional tabledata.list response (see test_read_schema_reorder_rejected).
    # So a single query issues two tables.get in total (the analysis snapshot and the pre-read drift check),
    # and no more, and it does not mint a second OAuth token.
    mock_reset()
    assert (
        node.query(f"SELECT * FROM {bq('test_paging')} ORDER BY i LIMIT 1 FORMAT TSV")
        == "0\tvalue0\n"
    )
    assert len(mock_stats()["schema_requests"]) == 2

    # The token is minted once and reused for schema inference, the pre-read drift check, and execution.
    creds = (
        "client_id = 'test-client-id.apps.googleusercontent.com', "
        "client_secret = 'test-client-secret', "
        "refresh_token = 'test-refresh-token', "
        f"token_url = '{BASE_URL}/token'"
    )
    mock_reset()
    assert node.query(f"SELECT count() FROM {bq('test_paging', creds=creds)}") == "10\n"
    assert len(mock_stats()["schema_requests"]) == 2
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


def query_from_log(query_id, event_type="QueryFinish"):
    node.query("SYSTEM FLUSH LOGS query_log")
    # TSVRaw: the default TSV output escapes quotes, which would break the
    # `key = '[HIDDEN]'` substring assertions below.
    return node.query(
        f"SELECT query FROM system.query_log WHERE query_id = '{query_id}' AND type = '{event_type}' FORMAT TSVRaw"
    )


def test_secret_masking_constant_key():
    # The *key* of a `key = value` argument does not have to be a literal either:
    # `BigQueryConfiguration::fromArguments` evaluates it as a constant expression, so
    # `concat('access', '_token') = '...'` can carry a credential. The masker cannot evaluate
    # the key, so it hides such an argument whole - neither the key expression nor the token
    # may reach `system.query_log`.
    #
    # In the table function form the analyzer folds the whole equality into a `UInt64` literal
    # before the arguments are parsed, so the query is rejected - but it is still logged, and
    # the original argument must be hidden in the log entry of the failed query.
    key_expr = "concat('access', '_token')"
    query_id = "bigquery-masking-concat-key-test"
    node.query_and_get_error(
        f"SELECT count() FROM bigquery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"{key_expr} = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    logged = query_from_log(query_id, event_type="ExceptionBeforeStart")
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "concat" not in logged
    assert "[HIDDEN]" in logged
    assert BASE_URL in logged

    # The engine form passes the raw argument ASTs to the configuration parser, so there the
    # constant-expression key authenticates - and must be masked in both the CREATE query log
    # entry and SHOW CREATE TABLE.
    node.query("DROP TABLE IF EXISTS bq_engine_expr_key")
    create_query_id = "bigquery-masking-concat-key-create"
    node.query(
        f"CREATE TABLE bq_engine_expr_key ENGINE = BigQuery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"{key_expr} = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        query_id=create_query_id,
    )
    create = node.query("SHOW CREATE TABLE bq_engine_expr_key")
    assert ACCESS_TOKEN not in create
    assert "concat" not in create
    assert "[HIDDEN]" in create
    assert node.query("SELECT count() FROM bq_engine_expr_key") == "10\n"
    logged = query_from_log(create_query_id)
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "concat" not in logged
    assert "[HIDDEN]" in logged
    node.query("DROP TABLE bq_engine_expr_key")


def test_secret_masking_fails_closed_on_invalid_arguments():
    # An invalid query is logged (`ExceptionBeforeStart`) before validation rejects it, so the
    # masker must fail closed on argument forms the parser would not accept.

    # A positional argument after a named collection is invalid, but could carry a credential
    # (e.g. a misplaced token) - it must be hidden in the log of the failed query.
    query_id = "bigquery-masking-collection-positional-test"
    node.query_and_get_error(
        f"SELECT count() FROM bigquery(bq_mock, '{ACCESS_TOKEN}')",
        query_id=query_id,
    )
    logged = query_from_log(query_id, event_type="ExceptionBeforeStart")
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "[HIDDEN]" in logged

    # An unknown key is rejected, but its value may be a credential under a mistyped key name -
    # only the values of the known non-secret keys stay visible.
    query_id = "bigquery-masking-unknown-key-test"
    node.query_and_get_error(
        f"SELECT count() FROM bigquery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"acess_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    logged = query_from_log(query_id, event_type="ExceptionBeforeStart")
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "acess_token = '[HIDDEN]'" in logged
    assert BASE_URL in logged

    # A 5th positional argument is also invalid and hidden.
    query_id = "bigquery-masking-extra-positional-test"
    node.query_and_get_error(
        f"SELECT count() FROM bigquery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"'{ACCESS_TOKEN}', 'stray-secret', base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    logged = query_from_log(query_id, event_type="ExceptionBeforeStart")
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "stray-secret" not in logged
    assert "[HIDDEN]" in logged

    # A positional argument landing on a slot already claimed by a `key = value` argument is
    # invalid too: the parser maps positionals onto the 'project', 'dataset', 'table' and
    # 'access_token' slots strictly by ordinal, so this lone positional is a would-be 'project'
    # while 'project' is already named - it may well be a misplaced token and must be hidden.
    query_id = "bigquery-masking-occupied-slot-test"
    node.query_and_get_error(
        f"SELECT count() FROM bigquery(project = '{PROJECT}', dataset = '{DATASET}', "
        f"table = 'test_paging', '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    logged = query_from_log(query_id, event_type="ExceptionBeforeStart")
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "[HIDDEN]" in logged
    assert BASE_URL in logged

    # The same for the engine form, which is masked by the table-engine finder.
    node.query("DROP TABLE IF EXISTS bq_engine_occupied_slot")
    query_id = "bigquery-masking-occupied-slot-engine-test"
    node.query_and_get_error(
        f"CREATE TABLE bq_engine_occupied_slot ENGINE = BigQuery(project = '{PROJECT}', "
        f"dataset = '{DATASET}', table = 'test_paging', '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    logged = query_from_log(query_id, event_type="ExceptionBeforeStart")
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert "[HIDDEN]" in logged

    # A key that is a constant expression may claim any slot, so no positional argument can be
    # trusted: they all fail closed, even the ones that would be 'project' and 'dataset'.
    query_id = "bigquery-masking-unreadable-key-positional-test"
    node.query_and_get_error(
        f"SELECT count() FROM bigquery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"concat('access', '_token') = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')",
        query_id=query_id,
    )
    logged = query_from_log(query_id, event_type="ExceptionBeforeStart")
    assert logged != ""
    assert ACCESS_TOKEN not in logged
    assert PROJECT not in logged
    assert "[HIDDEN]" in logged


def test_named_target_arguments():
    # `project`, `dataset` and `table` are first-class `key = value` arguments in the
    # non-collection form, for both the table function and the engine.
    assert (
        node.query(
            f"SELECT count() FROM bigquery(project = '{PROJECT}', dataset = '{DATASET}', "
            f"table = 'test_paging', access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
        )
        == "10\n"
    )
    node.query("DROP TABLE IF EXISTS bq_named_target")
    node.query(
        f"CREATE TABLE bq_named_target ENGINE = BigQuery(project = '{PROJECT}', dataset = '{DATASET}', "
        f"table = 'test_paging', access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert node.query("SELECT count() FROM bq_named_target") == "10\n"
    node.query("DROP TABLE bq_named_target")

    # A positional argument clashing with a named one is rejected instead of silently
    # targeting the positional value.
    error = node.query_and_get_error(
        f"SELECT count() FROM bigquery('{PROJECT}', '{DATASET}', 'test_paging', "
        f"table = 'other_table', access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert "specified both positionally" in error

    # Repeating a `key = value` argument is rejected as well.
    error = node.query_and_get_error(
        f"SELECT count() FROM bigquery(project = '{PROJECT}', dataset = '{DATASET}', "
        f"table = 'test_paging', table = 'other_table', access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert "more than once" in error

    # Named target arguments alone are not enough: the target must be complete.
    error = node.query_and_get_error(
        f"SELECT count() FROM bigquery(project = '{PROJECT}', dataset = '{DATASET}', "
        f"access_token = '{ACCESS_TOKEN}', base_url = '{BASE_URL}')"
    )
    assert "requires the 'project', 'dataset' and 'table' arguments" in error


def test_unsupported_table_types():
    # Only native tables can be read; views, materialized views and external tables are rejected
    # up front with a clear error, matching the documented contract.
    for table, type_ in [("a_view", "VIEW"), ("a_matview", "MATERIALIZED_VIEW")]:
        error = node.query_and_get_error(f"SELECT * FROM {bq(table)}")
        assert "cannot be read directly" in error
        assert type_ in error


def test_unknown_field_mode_rejected():
    # Only NULLABLE, REQUIRED and REPEATED are valid BigQuery field modes. A schema carrying any
    # other mode (a malformed response, or an enum value added to the API later) must be rejected
    # instead of being silently read as a nullable scalar with the wrong semantics. A missing mode
    # still defaults to NULLABLE (every other mock table relies on that).
    error = node.query_and_get_error(f"SELECT * FROM {bq('test_bad_mode')}")
    assert "unknown mode" in error
    assert "OPTIONAL" in error


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


def test_sync_remote_read_fans_out_to_max_distributed_connections():
    # `BigQuery` is a remote storage that reads through the generic `IStorage::read`, which bounds the
    # post-read resize by the number of threads that will consume its output. For a synchronous remote
    # read (`async_socket_for_remote = 0`) a thread blocks on the socket instead of running, so
    # `InterpreterSelectQuery` / `PlannerJoinTree` raise that budget from `max_threads` to
    # `max_distributed_connections`; the resize must follow it there instead of stopping at `max_threads`.
    # `EXPLAIN PIPELINE` builds the plan without reading any rows.
    plan = node.query(
        f"EXPLAIN PIPELINE SELECT * FROM {bq('test_types')}",
        settings={
            "async_socket_for_remote": 0,
            "max_distributed_connections": 16,
            "max_threads": 2,
            "max_threads_min_free_memory_per_thread": 0,
        },
    )
    assert "Resize 1 → 16" in plan, plan

    # An asynchronous remote read does not block a thread on the socket, so its budget is `max_threads`
    # and an absurd `max_streams_to_max_threads_ratio` must not widen the output past it.
    plan = node.query(
        f"EXPLAIN PIPELINE SELECT * FROM {bq('test_types')}",
        settings={
            "async_socket_for_remote": 1,
            "max_threads": 2,
            "max_streams_to_max_threads_ratio": 1000000,
            "max_threads_min_free_memory_per_thread": 0,
        },
    )
    assert "Resize 1 → 2" in plan, plan
