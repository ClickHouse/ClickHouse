import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[]
)


@pytest.fixture(scope="module")
def started_cluster(request):
    try:

        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def check_read_rows(query_id, read_rows):
    node.query("SYSTEM FLUSH LOGS")
    real_read_rows = node.query(
        f"SELECT read_rows FROM system.query_log WHERE type = 'QueryFinish' and query_id = '{query_id}'"
    )
    assert real_read_rows == str(read_rows) + "\n"


def test_simple_range(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number=1 FORMAT Values",
        query_id="test_equal"
    )
    assert response == "(1)"
    check_read_rows("test_equal", 1)

    response = node.query(
        "SELECT * FROM system.numbers WHERE number > 1 and number < 6 FORMAT Values",
        query_id="test_single_range"
    )
    assert response == "(2),(3),(4),(5)"
    check_read_rows("test_single_range", 4)


def test_between(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number between 1 and 6 FORMAT Values",
        query_id="test_between"
    )
    assert response == "(1),(2),(3),(4),(5),(6)"
    check_read_rows("test_between", 6)


def test_blank_range(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number < 1 and number > 6 FORMAT Values",
        query_id="test_blank_range"
    )
    assert response == ""
    check_read_rows("test_blank_range", 0)


def test_in(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number in (2, 3) FORMAT Values",
        query_id="test_in_simple"
    )
    assert response == "(2),(3)"
    check_read_rows("test_in_simple", 2)

    response = node.query(
        "SELECT * FROM system.numbers WHERE number in (2, 3, 3) FORMAT Values",
        query_id="test_in_with_duplicated_values"
    )
    assert response == "(2),(3)"
    check_read_rows("test_in_with_duplicated_values", 2)

    response = node.query(
        "SELECT * FROM system.numbers WHERE number in (2, 3, 1) FORMAT Values",
        query_id="test_in_with_unordered_values"
    )
    assert response == "(1),(2),(3)"
    check_read_rows("test_in_with_unordered_values", 3)


def test_not_in(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number not in (2, 3) limit 3 FORMAT Values",
        query_id="test_not_in"
    )
    assert response == "(0),(1),(4)"
    check_read_rows("test_not_in", 3)


def test_and(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number in (2, 4) and number > 2 FORMAT Values",
        query_id="test_and"
    )
    assert response == "(4)"
    check_read_rows("test_and", 1)


def test_or(started_cluster):
    response = node.query(
        """SELECT 
                * 
            FROM 
                system.numbers 
            WHERE 
                (number > 1 and number < 3) or (number in (4, 6)) or (number > 7 and number < 9) 
            FORMAT Values""",
        query_id="test_simple_or"
    )
    assert response == "(2),(4),(6),(8)"
    check_read_rows("test_simple_or", 4)

    response = node.query(
        "SELECT * FROM system.numbers WHERE (number > 1 and number < 3) or (number < 6) FORMAT Values",
        query_id="test_or_with_overlapping_ranges"
    )
    assert response == "(0),(1),(2),(3),(4),(5)"
    check_read_rows("test_or_with_overlapping_ranges", 6)


def test_not(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE not (number > 1 and number < 3) limit 5 FORMAT Values",
        query_id="test_not"
    )
    assert response == "(0),(1),(3),(4),(5)"
    check_read_rows("test_not", 5)


def test_true_or_false(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number < 3 and 1 limit 5 FORMAT Values",
        query_id="test_true"
    )
    assert response == "(0),(1),(2)"
    check_read_rows("test_true", 3)

    response = node.query(
        "SELECT * FROM system.numbers WHERE number < 3 and 0 FORMAT Values",
        query_id="test_false"
    )
    assert response == ""
    check_read_rows("test_false", 0)


def test_limit(started_cluster):
    response = node.query(
        "SELECT * FROM system.numbers WHERE number > 2 limit 1 FORMAT Values",
        query_id="test_simple_limit"
    )
    assert response == "(3)"
    check_read_rows("test_simple_limit", 1)

    response = node.query(
        "SELECT * FROM system.numbers WHERE number not in (2, 3) limit 1 FORMAT Values",
        query_id="test_limit_with_multi_ranges"
    )
    assert response == "(0)"
    check_read_rows("test_limit_with_multi_ranges", 1)

    response = node.query(
        "SELECT * FROM system.numbers WHERE number not in (2, 3) limit 1, 2 FORMAT Values",
        query_id="test_limit_with_offset"
    )
    assert response == "(1),(4)"
    check_read_rows("test_limit_with_offset", 3)


def test_subquery(started_cluster):
    response = node.query(
        """SELECT 
                * 
            FROM 
                (select * FROM system.numbers) as n  
            WHERE 
                number = 1 
            FORMAT Values""",
        query_id="test_simple_subquery"
    )
    assert response == "(1)"
    check_read_rows("test_simple_subquery", 1)

    response = node.query(
        """SELECT 
                * 
            FROM 
                (select * FROM system.numbers WHERE number < 4) AS n
            WHERE 
                number > 1 
            FORMAT Values""",
        query_id="test_subquery_with_predicate"
    )
    assert response == "(2),(3)"
    check_read_rows("test_subquery_with_predicate", 2)


def test_multi_streams(started_cluster):
    response = node.query(
        """SELECT 
                * 
            FROM 
                system.numbers_mt 
            WHERE 
                number > 1 and number < 7 
            ORDER BY 
                number 
            FORMAT Values 
            settings max_block_size=2""",
        query_id="test_multi_streams"
    )
    assert response == "(2),(3),(4),(5),(6)"
    check_read_rows("test_multi_streams", 5)

    response = node.query(
        """SELECT 
                * 
            FROM 
                system.numbers_mt
            WHERE 
                (number > 1 and number < 3) or (number in (4, 6)) or (number > 7 and number < 10) 
            ORDER BY 
                number 
            FORMAT Values 
            settings max_block_size=2""",
        query_id="test_multi_streams_with_multi_ranges"
    )
    assert response == "(2),(4),(6),(8),(9)"
    check_read_rows("test_multi_streams", 5)
