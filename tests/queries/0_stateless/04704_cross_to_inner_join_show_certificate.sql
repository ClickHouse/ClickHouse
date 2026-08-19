-- Tags: no-fasttest

SET enable_analyzer = 1;
SET query_plan_merge_filter_into_join_condition = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64) ENGINE = Log;

-- `showCertificate` must not become a join key.
SELECT count() = 0 FROM (
EXPLAIN QUERY TREE run_passes = 1
SELECT count() FROM l, r
WHERE concat(toString(l.a), showCertificate()['version'])
    = concat(toString(r.a), showCertificate()['version'])
SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

DROP TABLE l;
DROP TABLE r;
