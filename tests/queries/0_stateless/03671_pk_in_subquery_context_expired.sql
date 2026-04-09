-- Issue: https://github.com/ClickHouse/ClickHouse/issues/89433

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS join_engine;

CREATE TABLE tbl
(
    `id1` LowCardinality(String),
    `id2` LowCardinality(String),
    `v` Int64
)
ENGINE = MergeTree
ORDER BY (id1, id2, v);
INSERT INTO tbl VALUES ('a', 'b', 1);
CREATE TABLE join_engine
(
    `id1` LowCardinality(String),
    `id2` LowCardinality(String),
    `v` Int64
)
ENGINE = Join(ANY, LEFT, id1, id2);
INSERT INTO join_engine VALUES ('a', 'b', 1);

WITH cte AS
    (
        SELECT id2
        FROM tbl
        WHERE joinGet(currentDatabase() || '.join_engine', 'v', id1, id2) = tbl.v
    )
SELECT uniq(id2) AS count
FROM
(
    -- NOTE: the bug is reproduced only because due to
    -- enable_global_with_statement adds "cte" here, but likely it will be
    -- fixed one day... so I've added another test below that does not rely
    -- on this fact
    SELECT *
    FROM tbl AS e
    WHERE joinGet(currentDatabase() || '.join_engine', 'v', id1, id2) = e.v
)
WHERE id2 IN (
    SELECT id2
    FROM cte
)
UNION ALL
SELECT uniq(id2) AS count
FROM cte;

SELECT 'Testing w/o relying on enable_global_with_statement...';
--
-- The same as before, but without relying on enable_global_with_statement
--
SELECT uniq(id2) AS count
FROM
(
    WITH cte AS
        (
            SELECT id2
            FROM tbl
            WHERE joinGet(currentDatabase() || '.join_engine', 'v', id1, id2) = tbl.v
        )
    SELECT *
    FROM tbl AS e
    WHERE joinGet(currentDatabase() || '.join_engine', 'v', id1, id2) = e.v
)
WHERE id2 IN (
    SELECT id2
    FROM
    (
        SELECT id2
        FROM tbl
        WHERE joinGet(currentDatabase() || '.join_engine', 'v', id1, id2) = tbl.v
    )
)
UNION ALL
SELECT uniq(id2) AS count
FROM
(
    SELECT id2
    FROM tbl
    WHERE joinGet(currentDatabase() || '.join_engine', 'v', id1, id2) = tbl.v
);
