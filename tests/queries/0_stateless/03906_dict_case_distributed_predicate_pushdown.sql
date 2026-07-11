-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/87403
-- Dictionary + CASE + distributed table: predicate pushdown should not filter out rows incorrectly.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_dict_dist_local;
DROP DICTIONARY IF EXISTS d_dict_dist;
DROP TABLE IF EXISTS t_dict_dist;

CREATE TABLE t_dict_dist_local
(
    id Int64,
    c String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_dict_dist_local VALUES (1, 'same'), (2, 'same');

CREATE DICTIONARY d_dict_dist
(
    id Int64,
    d String
) PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY '
    SELECT * FROM (
        SELECT toInt64(1) AS id, ''alpha'' AS d
        UNION ALL
        SELECT toInt64(2) AS id, ''beta'' AS d
    )
'))
LAYOUT(FLAT())
LIFETIME(0);

CREATE TABLE t_dict_dist AS t_dict_dist_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_dict_dist_local);

SELECT
    id,
    c,
    dictGet(concat(currentDatabase(), '.d_dict_dist'), 'd', id) AS d,
    CASE
        WHEN c = 'same' AND d = 'gamma' THEN 'SHOULD NOT HAPPEN'
        WHEN c = 'same' THEN 'SHOULD ALWAYS HAPPEN'
    END AS filter_value
FROM (
    SELECT * FROM t_dict_dist
)
WHERE filter_value = 'SHOULD ALWAYS HAPPEN'
ORDER BY id;

DROP TABLE t_dict_dist;
DROP DICTIONARY d_dict_dist;
DROP TABLE t_dict_dist_local;
