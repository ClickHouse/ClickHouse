-- Correlated scalar subqueries can be used in lightweight UPDATE assignments.

SET enable_analyzer = 1;
SET enable_lightweight_update = 1;
SET mutations_sync = 1;

DROP TABLE IF EXISTS t_correlated_update_source;
DROP TABLE IF EXISTS t_correlated_update_target;

CREATE TABLE t_correlated_update_target
(
    id UInt64,
    region UInt8,
    company_name Nullable(String),
    score Nullable(Int64)
)
ENGINE = MergeTree
ORDER BY (region, id)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_correlated_update_source
(
    id UInt64,
    region UInt8,
    company_name String,
    score Int64
)
ENGINE = MergeTree
ORDER BY (region, id);

INSERT INTO t_correlated_update_target VALUES
    (1, 1, 'old', 0),
    (1, 2, 'old', 0),
    (2, 1, 'old', 0),
    (3, 1, 'keep', 0);

INSERT INTO t_correlated_update_source VALUES
    (1, 1, 'Acme', 10),
    (1, 2, 'Globex', 20),
    (2, 1, 'Acme EU', 30);

-- Multiple assignments and a composite correlation key.
UPDATE t_correlated_update_target
SET
    company_name =
    (
        SELECT s.company_name
        FROM t_correlated_update_source AS s
        WHERE s.id = t_correlated_update_target.id
          AND s.region = t_correlated_update_target.region
    ),
    score =
    (
        SELECT max(s.score)
        FROM t_correlated_update_source AS s
        WHERE s.id = t_correlated_update_target.id
          AND s.region = t_correlated_update_target.region
    )
WHERE (id, region) IN (SELECT id, region FROM t_correlated_update_source);

SELECT id, region, company_name, score FROM t_correlated_update_target ORDER BY region, id;

-- A scalar subquery with no matching source row produces a nullable result.
UPDATE t_correlated_update_target
SET
    company_name =
    (
        SELECT s.company_name
        FROM t_correlated_update_source AS s
        WHERE s.id = t_correlated_update_target.id
          AND s.region = t_correlated_update_target.region
    ),
    score =
    (
        SELECT max(s.score)
        FROM t_correlated_update_source AS s
        WHERE s.id = t_correlated_update_target.id
          AND s.region = t_correlated_update_target.region
    )
WHERE 1;

SELECT id, region, company_name, score FROM t_correlated_update_target ORDER BY region, id;

DROP TABLE t_correlated_update_source;
DROP TABLE t_correlated_update_target;
