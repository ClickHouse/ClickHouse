import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/format_alter_operations_with_parentheses.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_alter():
    INPUT = """
SELECT '--- Alter commands in parens';
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr GROUP BY some_key), (ADD COLUMN a Int64)');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr TO VOLUME \\'vol1\\', expr2 + INTERVAL 2 YEAR TO VOLUME \\'vol2\\'), (DROP COLUMN c)');

SELECT '--- Check only consistent parens around alter commands are accepted';
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), DROP COLUMN c'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, (DROP COLUMN c)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), (DROP COLUMN c)');
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, DROP COLUMN c'); -- Make sure it is backward compatible
"""

    EXPECTED_OUTPUT = """--- Alter commands in parens
ALTER TABLE a\\n    (MODIFY TTL expr GROUP BY some_key),\\n    (ADD COLUMN `a` Int64)
ALTER TABLE a\\n    (MODIFY TTL expr TO VOLUME \\'vol1\\', expr2 + toIntervalYear(2) TO VOLUME \\'vol2\\'),\\n    (DROP COLUMN c)
--- Check only consistent parens around alter commands are accepted
ALTER TABLE a\\n    (DROP COLUMN b),\\n    (DROP COLUMN c)
ALTER TABLE a\\n    (DROP COLUMN b),\\n    (DROP COLUMN c)
"""
    result = node.query(INPUT)
    assert result == EXPECTED_OUTPUT
