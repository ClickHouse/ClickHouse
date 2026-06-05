-- Tags: no-fasttest
-- no-fasttest: requires ICU for collation support.

-- { echo }

DROP TABLE IF EXISTS test;
CREATE TABLE test (x String) ENGINE = Memory;
INSERT INTO test VALUES ('1'), ('01'), ('2'), ('10'), ('9');

-- numeric collation, single sort column
SELECT x FROM (SELECT x FROM test ORDER BY x COLLATE 'en-u-kn-true' LIMIT 1 WITH TIES) ORDER BY x;

-- numeric collation, larger tie group
SELECT x FROM (SELECT x FROM (SELECT arrayJoin(['1', '01', '001', '2', '02', '3']) AS x) ORDER BY x COLLATE 'en-u-kn-true' LIMIT 1 WITH TIES) ORDER BY x;

-- collation as a secondary sort key
SELECT k, x FROM (SELECT k, x FROM (SELECT arrayJoin([(0, '1'), (0, '01'), (1, '2')]) AS t, t.1 AS k, t.2 AS x) ORDER BY k, x COLLATE 'en-u-kn-true' LIMIT 1 WITH TIES) ORDER BY k, x;

-- fractional limit, numeric collation
SELECT x FROM (SELECT x FROM test ORDER BY x COLLATE 'en-u-kn-true' LIMIT 0.2 WITH TIES) ORDER BY x;

-- fractional limit with offset, numeric collation
SELECT x FROM (SELECT x FROM (SELECT arrayJoin(['1', '01', '2', '02', '3', '03']) AS x) ORDER BY x COLLATE 'en-u-kn-true' LIMIT 2, 0.2 WITH TIES) ORDER BY x;

-- Negative LIMIT WITH TIES is not supported yet.
SELECT x FROM (SELECT x FROM (SELECT arrayJoin(['3', '03', '003', '2', '02', '1']) AS x) ORDER BY x COLLATE 'en-u-kn-true' LIMIT -1 WITH TIES) ORDER BY x; -- { serverError NOT_IMPLEMENTED }

SELECT k, x FROM (SELECT k, x FROM (SELECT arrayJoin([(0, '1'), (1, '3'), (1, '03'), (1, '003')]) AS t, t.1 AS k, t.2 AS x) ORDER BY k, x COLLATE 'en-u-kn-true' LIMIT -1 WITH TIES) ORDER BY k, x; -- { serverError NOT_IMPLEMENTED }

DROP TABLE test;
