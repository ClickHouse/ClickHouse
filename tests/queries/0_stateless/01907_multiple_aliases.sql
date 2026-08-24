DROP TABLE IF EXISTS t;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE t (d Date, z UInt32) ENGINE = MergeTree(d, (z), 1);

INSERT INTO t VALUES ('2017-01-01', 1);

WITH (d < '2018-01-01') AND (d < '2018-01-02') AS x
SELECT 1
FROM t
WHERE x;

DROP TABLE t;
