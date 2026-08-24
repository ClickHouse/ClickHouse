-- the test from simPod, https://github.com/ClickHouse/ClickHouse/issues/5608

DROP TABLE IF EXISTS joint; -- the table name from the original issue.
DROP TABLE IF EXISTS t;

CREATE TABLE IF NOT EXISTS joint
(
    id    UUID,
    value LowCardinality(String)
)
ENGINE = Join (ANY, LEFT, id);

CREATE TABLE IF NOT EXISTS t
(
    id    UUID,
    d     DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(d)
ORDER BY id;

insert into joint VALUES ('00000000-0000-0000-0000-000000000000', 'yo');
insert into t VALUES ('00000000-0000-0000-0000-000000000000', now());

SELECT id FROM t
ANY LEFT JOIN joint ON t.id = joint.id;

DROP TABLE joint;
DROP TABLE t;
