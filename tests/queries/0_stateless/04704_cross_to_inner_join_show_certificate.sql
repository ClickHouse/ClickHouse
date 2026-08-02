-- Tags: no-fasttest
-- no-fasttest: showCertificate() throws SUPPORT_IS_DISABLED when the build has no OpenSSL, and the
-- fast build does not initialize contrib/openssl. The throw is in create(), so even EXPLAIN fails.

SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 0;

DROP TABLE IF EXISTS l SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS r SETTINGS ignore_drop_queries_probability = 0;

CREATE TABLE l (a UInt64, b UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64, b UInt64) ENGINE = Log;

INSERT INTO l SELECT number % 16, number % 4 FROM numbers(500);
INSERT INTO r SELECT number % 16, number % 4 FROM numbers(500);

-- `showCertificate` reported no determinism predicate at all and now declares `isDeterministic` and
-- `isServerConstant`. It never returns a constant column, so it is not folded on a single-node query
-- and needs no `remote` vehicle.
SELECT '-- showCertificate() is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r
    WHERE concat(toString(l.a), showCertificate()['version'])
        = concat(toString(r.a), showCertificate()['version'])
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

-- A deterministic predicate over the same tables must still be rewritten, so the row above fails for
-- showCertificate() rather than for the fixture.
SELECT '-- a deterministic predicate is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a = r.a SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

DROP TABLE l SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE r SETTINGS ignore_drop_queries_probability = 0;
