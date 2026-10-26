DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

SET allow_suspicious_low_cardinality_types = 1;
SET enable_analyzer = 1;
CREATE TABLE t0 (c0 LowCardinality(Int)) ENGINE = MergeTree() ORDER BY (c0);
CREATE TABLE t1 (c0 Nullable(Int)) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE t0 (c0) VALUES (0), (1);
INSERT INTO TABLE t0 (c0) VALUES (-10), (10);
INSERT INTO TABLE t0 (c0) VALUES (0), (-1);
INSERT INTO TABLE t1 (c0) VALUES (1), (2);

SET read_in_order_use_virtual_row = 1;


SELECT CAST(c0, 'Int32') a FROM t0 ORDER BY a;

SELECT '-';
SELECT * FROM t0 JOIN t1 ON t1.c0.null = t0.c0
ORDER BY t0.c0, t1.c0;

SELECT '-';

SELECT * FROM t0 JOIN t1 ON t1.c0.null = t0.c0
ORDER BY t0.c0, t1.c0
SETTINGS join_algorithm = 'full_sorting_merge';
