-- Tags: no-fasttest

SET enable_analyzer = 1;
SET query_plan_merge_filter_into_join_condition = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64) ENGINE = Log;

-- `showCertificate` must not become a join key: its value is the executing node's own
-- certificate, so the two sides of a distributed join can disagree. Before this fix, the
-- rewrite turned this comma join into an `INNER` join keyed on the `concat` expression.
SELECT count() = 0 FROM (
EXPLAIN QUERY TREE run_passes = 1
SELECT count() FROM l, r
WHERE concat(toString(l.a), showCertificate()['version'])
    = concat(toString(r.a), showCertificate()['version'])
SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

DROP TABLE l;
DROP TABLE r;
