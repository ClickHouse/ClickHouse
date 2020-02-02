-- Just testing syntax for now.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dest;
DROP TABLE IF EXISTS pipe;

CREATE TABLE src(v UInt64) ENGINE = Null;
CREATE TABLE dest(v UInt64) Engine = MergeTree() ORDER BY v;

CREATE MATERIALIZED VIEW pipe TO dest AS
SELECT v FROM src;

INSERT INTO src VALUES (1), (2), (3);

SET allow_experimental_alter_materialized_view_structure = 1;

-- Live alter which changes query logic and adds an extra column.
-- This is not implemented yet and this test is just a draft.
ALTER TABLE pipe
    MODIFY QUERY
    SELECT
        v * 2 as v,
        1 as v2
    FROM src; -- { serverError 48 }

INSERT INTO src VALUES (1), (2), (3);

SELECT * FROM dest ORDER BY v;

ALTER TABLE dest
    ADD COLUMN v2 UInt64;

INSERT INTO src VALUES (42);
SELECT * FROM dest ORDER BY v;

DROP TABLE src;
DROP TABLE dest;
DROP TABLE pipe;
