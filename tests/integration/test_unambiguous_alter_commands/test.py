import pytest
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
alter_node = cluster.add_instance(
    "alter_node",
    main_configs=[
        "configs/format_alter_operations_with_parentheses.xml",
    ],
)

ttl_node = cluster.add_instance(
    "ttl_node",
    main_configs=[
        "configs/format_ttl_expressions_with_parentheses.xml",
    ],
)

both_node = cluster.add_instance(
    "both_node",
    main_configs=[
        "configs/format_alter_operations_with_parentheses.xml",
        "configs/format_ttl_expressions_with_parentheses.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


QUERIES = """
SELECT '--- Alter commands in parens';
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr GROUP BY some_key), (ADD COLUMN a Int64)');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr TO VOLUME \\'vol1\\', expr2 + INTERVAL 2 YEAR TO VOLUME \\'vol2\\'), (DROP COLUMN c)');

SELECT '--- TTL expressions in parens';
SELECT formatQuery('ALTER TABLE a MODIFY TTL (expr GROUP BY some_key), (expr2)');
SELECT formatQuery('ALTER TABLE a MODIFY TTL (expr GROUP BY some_key), MATERIALIZE TTL');

SELECT '--- Check only consistent parens around alter commands are accepted';
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), DROP COLUMN c'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, (DROP COLUMN c)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), (DROP COLUMN c)');
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, DROP COLUMN c'); -- Make sure it is backward compatible

SELECT '--- Check only consistent parens around TTL expressions are accepted';
-- This is a tricky one. We not supposed to allow inconsistent parens among TTL expressions, however "(expr2)" is a
-- valid expression on its own, thus the parens doesn't belong to the TTL expression, but to the expression, same as
-- it would like "SELECT (expr2)".
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, (expr2))');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, (((expr2))))');
-- Almost same as above, but "expr2 GROUP BY expr3" cannot be parsed as an expression, thus the parens has to be
-- parsed as part of the TTL expression which results in inconsistent parens.
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), expr2)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, (expr2 GROUP BY expr3))'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), expr2 GROUP BY expr3)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), (expr2))');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, expr2)'); -- Make sure it is backward compatible
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), (expr2 GROUP BY expr3))');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, expr2 GROUP BY expr3)'); -- Make sure it is backward compatible
"""


def test_alter():
    EXPECTED_OUTPUT = """--- Alter commands in parens
ALTER TABLE a\\n    (MODIFY TTL expr GROUP BY some_key),\\n    (ADD COLUMN `a` Int64)
ALTER TABLE a\\n    (MODIFY TTL expr TO VOLUME \\'vol1\\', expr2 + toIntervalYear(2) TO VOLUME \\'vol2\\'),\\n    (DROP COLUMN c)
--- TTL expressions in parens
ALTER TABLE a\\n    (MODIFY TTL expr GROUP BY some_key, expr2)
ALTER TABLE a\\n    (MODIFY TTL expr GROUP BY some_key),\\n    (MATERIALIZE TTL)
--- Check only consistent parens around alter commands are accepted
ALTER TABLE a\\n    (DROP COLUMN b),\\n    (DROP COLUMN c)
ALTER TABLE a\\n    (DROP COLUMN b),\\n    (DROP COLUMN c)
--- Check only consistent parens around TTL expressions are accepted
ALTER TABLE a\\n    (MODIFY TTL expr, expr2)
ALTER TABLE a\\n    (MODIFY TTL expr, expr2)
ALTER TABLE a\\n    (MODIFY TTL expr, expr2)
ALTER TABLE a\\n    (MODIFY TTL expr, expr2)
ALTER TABLE a\\n    (MODIFY TTL expr, expr2 GROUP BY expr3)
ALTER TABLE a\\n    (MODIFY TTL expr, expr2 GROUP BY expr3)
"""
    result = alter_node.query(QUERIES)
    assert result == EXPECTED_OUTPUT


def test_ttl():
    EXPECTED_OUTPUT = """--- Alter commands in parens
ALTER TABLE a\\n    MODIFY TTL (expr GROUP BY some_key),\\n    ADD COLUMN `a` Int64
ALTER TABLE a\\n    MODIFY TTL (expr TO VOLUME \\'vol1\\'), (expr2 + toIntervalYear(2) TO VOLUME \\'vol2\\'),\\n    DROP COLUMN c
--- TTL expressions in parens
ALTER TABLE a\\n    MODIFY TTL (expr GROUP BY some_key), (expr2)
ALTER TABLE a\\n    MODIFY TTL (expr GROUP BY some_key),\\n    MATERIALIZE TTL
--- Check only consistent parens around alter commands are accepted
ALTER TABLE a\\n    DROP COLUMN b,\\n    DROP COLUMN c
ALTER TABLE a\\n    DROP COLUMN b,\\n    DROP COLUMN c
--- Check only consistent parens around TTL expressions are accepted
ALTER TABLE a\\n    MODIFY TTL (expr), (expr2)
ALTER TABLE a\\n    MODIFY TTL (expr), (expr2)
ALTER TABLE a\\n    MODIFY TTL (expr), (expr2)
ALTER TABLE a\\n    MODIFY TTL (expr), (expr2)
ALTER TABLE a\\n    MODIFY TTL (expr), (expr2 GROUP BY expr3)
ALTER TABLE a\\n    MODIFY TTL (expr), (expr2 GROUP BY expr3)
"""
    result = ttl_node.query(QUERIES)
    assert result == EXPECTED_OUTPUT


def test_both():
    EXPECTED_OUTPUT = """--- Alter commands in parens
ALTER TABLE a\\n    (MODIFY TTL (expr GROUP BY some_key)),\\n    (ADD COLUMN `a` Int64)
ALTER TABLE a\\n    (MODIFY TTL (expr TO VOLUME \\'vol1\\'), (expr2 + toIntervalYear(2) TO VOLUME \\'vol2\\')),\\n    (DROP COLUMN c)
--- TTL expressions in parens
ALTER TABLE a\\n    (MODIFY TTL (expr GROUP BY some_key), (expr2))
ALTER TABLE a\\n    (MODIFY TTL (expr GROUP BY some_key)),\\n    (MATERIALIZE TTL)
--- Check only consistent parens around alter commands are accepted
ALTER TABLE a\\n    (DROP COLUMN b),\\n    (DROP COLUMN c)
ALTER TABLE a\\n    (DROP COLUMN b),\\n    (DROP COLUMN c)
--- Check only consistent parens around TTL expressions are accepted
ALTER TABLE a\\n    (MODIFY TTL (expr), (expr2))
ALTER TABLE a\\n    (MODIFY TTL (expr), (expr2))
ALTER TABLE a\\n    (MODIFY TTL (expr), (expr2))
ALTER TABLE a\\n    (MODIFY TTL (expr), (expr2))
ALTER TABLE a\\n    (MODIFY TTL (expr), (expr2 GROUP BY expr3))
ALTER TABLE a\\n    (MODIFY TTL (expr), (expr2 GROUP BY expr3))
"""
    result = both_node.query(QUERIES)
    assert result == EXPECTED_OUTPUT
