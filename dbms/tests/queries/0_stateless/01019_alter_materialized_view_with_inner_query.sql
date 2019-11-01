DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;

CREATE TABLE src(v UInt64) ENGINE = Null;

CREATE MATERIALIZED VIEW mv
(
    v UInt64
) ENGINE=MergeTree() ORDER BY v
AS
SELECT v FROM src;

INSERT INTO src VALUES (1), (2), (3);

SELECT * FROM mv ORDER BY v;

-- Live alter which changes query logic and adds an extra column.
ALTER TABLE mv
    ADD COLUMN v2 UInt64,
    MODIFY QUERY
        SELECT
            v * 2 as v,
            1 as v2
        FROM src;

INSERT INTO src VALUES (1), (2), (3);

SELECT * FROM mv ORDER BY v, v2;

DROP TABLE src;
DROP TABLE mv;