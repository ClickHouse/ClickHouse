DROP TABLE IF EXISTS stripelog_alter_add_column_edges;

CREATE TABLE stripelog_alter_add_column_edges
(
    a UInt64,
    s String
)
ENGINE = StripeLog;

INSERT INTO stripelog_alter_add_column_edges VALUES (1, 'one');

ALTER TABLE stripelog_alter_add_column_edges ADD COLUMN b UInt64 DEFAULT a + 10;
INSERT INTO stripelog_alter_add_column_edges (a, s) VALUES (2, 'two');

ALTER TABLE stripelog_alter_add_column_edges ADD COLUMN c UInt64 DEFAULT b * 2;
INSERT INTO stripelog_alter_add_column_edges (a, s) VALUES (3, 'three');

ALTER TABLE stripelog_alter_add_column_edges ADD COLUMN d UInt64 DEFAULT c * 2;
INSERT INTO stripelog_alter_add_column_edges (a, s) VALUES (4, 'four');

SELECT a, b, c, d
FROM stripelog_alter_add_column_edges
ORDER BY a
SETTINGS max_threads = 5, max_streams_to_max_threads_ratio = 1;

DETACH TABLE stripelog_alter_add_column_edges;
ATTACH TABLE stripelog_alter_add_column_edges;

SELECT sum(a), sum(b), sum(c), sum(d)
FROM stripelog_alter_add_column_edges;

DROP TABLE stripelog_alter_add_column_edges;
