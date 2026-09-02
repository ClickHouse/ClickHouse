DROP TABLE if exists test;

CREATE TABLE test
(
    uuid FixedString(16),
    id int,
    ns FixedString(16),
    dt DateTime64(6),
)
ENGINE = MergeTree
ORDER BY (id, dt, uuid);

ALTER TABLE test ADD PROJECTION mtlog_proj_source_reference (SELECT * ORDER BY substring(ns, 1, 5));

SHOW CREATE test;

drop table test;
