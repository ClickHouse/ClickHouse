import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")

URL_PREFIX = "tabular"


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()

        instance.http_query(
            """
            CREATE TABLE
                number (a UInt8, b UInt8)
            ENGINE = Memory
            """,
            method="POST",
        )
        instance.http_query(
            """
            INSERT INTO
                number
            VALUES
                (1, 2), (1, 3), (2, 3),
                (2, 1), (3, 4), (3, 1), (3, 5)
            """,
            method="POST",
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_happy_result():
    response = instance.http_request(
        url=f"{URL_PREFIX}/number.csv", params={"limit": 3}
    )

    assert response.status_code == 200
    assert response.content == b"1,2\n1,3\n2,3\n"


@pytest.mark.parametrize(
    ["url", "params", "sql_query"],
    [
        pytest.param(
            "number.csv",
            {"limit": 3},
            "SELECT * FROM number LIMIT 3 FORMAT CSV",
            id="default",
        ),
        pytest.param(
            "system/numbers.tsv",
            {"limit": 10},
            "SELECT * FROM system.numbers LIMIT 10 FORMAT TSV",
            id="database",
        ),
        pytest.param(
            "default/number",
            {"limit": 10, "format": "tsv"},
            "SELECT * FROM number LIMIT 10 FORMAT TSV",
            id="no format",
        ),
        pytest.param(
            "number",
            {"columns": "a"},
            "SELECT a FROM number",
            id="one column",
        ),
        pytest.param(
            "number",
            {"columns": "a", "select": "SELECT b +  11"},
            "SELECT a, b + 11 FROM number",
            id="two columns",
        ),
        pytest.param(
            "number",
            {"columns": "a", "select": "SELECT b +  11", "where": "a>1 AND b<=3"},
            "SELECT a, b + 11 FROM number WHERE a > 1 AND b <= 3",
            id="where",
        ),
        pytest.param(
            "number",
            {
                "columns": "a",
                "select": "SELECT b +  11",
                "where": "a>1 AND b<=3",
                "order": "a DESC, b ASC",
            },
            "SELECT a, b + 11 FROM number WHERE a > 1 AND b <= 3 ORDER BY a DESC, b",
            id="order by",
        ),
    ],
)
def test_scenarios(url, params, sql_query):
    response = instance.http_request(
        url=f"{URL_PREFIX}/{url}",
        params=params,
    )
    response.encoding = "UTF-8"

    expected_response = instance.http_query(sql_query, method="POST")

    assert response.status_code == 200
    assert response.text == expected_response


@pytest.mark.parametrize(
    ["query", "params", "sql_query"],
    [
        pytest.param("SELECT * FROM number", {}, "SELECT * FROM number", id="default"),
        pytest.param(
            "SELECT a FROM number",
            {"columns": "b", "limit": 3},
            "SELECT a, b FROM number LIMIT 3",
            id="columns",
        ),
        pytest.param(
            "SELECT a FROM number",
            {"select": "SELECT a * b, a + b, a / b, 5"},
            "SELECT a, a * b, a + b, a / b, 5 FROM number",
            id="select",
        ),
        pytest.param(
            "SELECT * FROM number WHERE a > 1",
            {"where": "b <= 3"},
            "SELECT * FROM number WHERE a > 1 AND b <= 3",
            id="where",
        ),
        pytest.param(
            "SELECT * FROM number WHERE a > 1 ORDER BY a ASC",
            {"where": "b <= 10", "order": "b DESC"},
            "SELECT * FROM number WHERE a > 1 AND b <= 10 ORDER BY a ASC, b DESC",
            id="order by",
        ),
    ],
)
def test_combining_params(query, params, sql_query):
    response = instance.http_query(query, params=params, method="POST")
    expected_response = instance.http_query(sql_query, method="POST")

    assert response == expected_response
