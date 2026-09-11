-- Tags: no-fasttest

SET enable_analyzer = 1;
SET query_plan_merge_filter_into_join_condition = 1;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64) ENGINE = Log;

-- `showCertificate` must not become a join key: its value is the executing node's own
-- certificate, so the two sides of a distributed join can disagree. In a distributed query
-- the function is not folded into a constant, so the comma join has to stay a cross join with
-- the comparison in the filter above it.
SELECT count() = 0 FROM (
EXPLAIN actions = 1
SELECT count() FROM l, remote('127.0.0.2', currentDatabase(), r) AS r
WHERE concat(toString(l.a), showCertificate()['version'])
    = concat(toString(r.a), showCertificate()['version'])
) WHERE explain ILIKE '%Join conditions%' OR explain ILIKE '%Type: INNER%';

-- A deterministic constant in the same place is a valid key.
SELECT count() = 1 FROM (
EXPLAIN actions = 1
SELECT count() FROM l, remote('127.0.0.2', currentDatabase(), r) AS r
WHERE concat(toString(l.a), 'x') = concat(toString(r.a), 'x')
) WHERE explain ILIKE '%Join conditions%';

DROP TABLE l;
DROP TABLE r;
