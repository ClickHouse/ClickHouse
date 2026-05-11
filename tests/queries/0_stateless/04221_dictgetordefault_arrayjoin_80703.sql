-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/80703
-- `dictGetOrDefault` + `arrayJoin` in WHERE on a `MergeTree` with a `PROJECTION` used to throw `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP DICTIONARY IF EXISTS tags_80703;
DROP TABLE IF EXISTS sync_local_80703;
DROP TABLE IF EXISTS tags_src_80703;

CREATE TABLE tags_src_80703
(
    p_id LowCardinality(String),
    site_name String
) ENGINE = MergeTree ORDER BY p_id;

INSERT INTO tags_src_80703 VALUES ('p1', 'site_a'), ('p2', 'site_b');

CREATE DICTIONARY tags_80703
(
    p_id String,
    site_name String
)
PRIMARY KEY p_id
SOURCE(CLICKHOUSE(TABLE tags_src_80703))
LAYOUT(HASHED())
LIFETIME(0);

CREATE TABLE sync_local_80703
(
    __partition LowCardinality(String),
    __sourcerows UInt64,
    __timestamp DateTime,
    p_id LowCardinality(String),
    cust_flags Array(String),
    PROJECTION sum_events
    (
        SELECT __timestamp, p_id, sum(__sourcerows)
        GROUP BY __timestamp, p_id
    )
)
ENGINE = MergeTree
PARTITION BY __partition
ORDER BY (__timestamp, p_id);

INSERT INTO sync_local_80703 VALUES ('part1', 5, '2025-05-16 10:00:00', 'p1', ['flag_a', 'flag_b']);
INSERT INTO sync_local_80703 VALUES ('part1', 3, '2025-05-17 11:00:00', 'p2', ['flag_a']);

SELECT
    arrayJoin(cust_flags) AS custs,
    sum(__sourcerows) AS total_events
FROM sync_local_80703
WHERE __timestamp >= '2025-05-15 02:00:00'
  AND __timestamp <  '2025-05-22 02:00:00'
  AND arrayJoin(cust_flags) IN ('flag_a')
  AND dictGetOrDefault('tags_80703', 'site_name', p_id, '') IN ('site_a', 'site_b')
GROUP BY 1
ORDER BY 1
LIMIT 5000;

DROP DICTIONARY tags_80703;
DROP TABLE sync_local_80703;
DROP TABLE tags_src_80703;
