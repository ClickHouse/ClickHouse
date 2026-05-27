-- Tags: zookeeper, no-parallel

DROP TABLE IF EXISTS tab_src SYNC;
DROP TABLE IF EXISTS tab_trigger SYNC;
DROP TABLE IF EXISTS tab_dst SYNC;
DROP TABLE IF EXISTS tab_mv_to_trigger SYNC;
DROP TABLE IF EXISTS tab_mv_to_dst SYNC;

CREATE TABLE tab_src
(
    id String,
    val UInt64,
    settled UInt8
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tab_src', '1')
ORDER BY id;

CREATE TABLE tab_trigger
(
    id String
)
ENGINE = Null;

CREATE MATERIALIZED VIEW tab_mv_to_trigger TO tab_trigger AS
SELECT id FROM tab_src WHERE settled = 1;

CREATE TABLE tab_dst
(
    id String,
    total_val UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tab_dst', '1')
ORDER BY id;

CREATE MATERIALIZED VIEW tab_mv_to_dst TO tab_dst AS
SELECT
    s.id AS id,
    sum(s.val) AS total_val
FROM tab_trigger AS q
INNER JOIN tab_src AS s ON s.id = q.id
GROUP BY s.id;

SET wait_for_part_commit_in_dependent_materialized_views = 1;

INSERT INTO tab_src VALUES ('a', 10, 0);
INSERT INTO tab_src VALUES ('a', 20, 0);
INSERT INTO tab_src VALUES ('a', 30, 1);

SELECT total_val FROM tab_dst;

DROP TABLE tab_mv_to_dst SYNC;
DROP TABLE tab_mv_to_trigger SYNC;
DROP TABLE tab_dst SYNC;
DROP TABLE tab_trigger SYNC;
DROP TABLE tab_src SYNC;
