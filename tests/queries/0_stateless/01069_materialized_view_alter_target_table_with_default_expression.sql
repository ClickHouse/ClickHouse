DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS mv_source;
DROP TABLE IF EXISTS mv_target;

CREATE TABLE mv_source (`a` UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE mv_target (`a` UInt64) ENGINE = MergeTree ORDER BY tuple();

CREATE MATERIALIZED VIEW mv TO mv_target AS SELECT * FROM mv_source;

INSERT INTO mv_source VALUES (1);

ALTER TABLE mv_target ADD COLUMN b UInt8 DEFAULT a + 1;
INSERT INTO mv_source VALUES (1),(2),(3);

SELECT * FROM mv ORDER BY a;
SELECT * FROM mv_target ORDER BY a;

DROP TABLE mv;
DROP TABLE mv_source;
DROP TABLE mv_target;
