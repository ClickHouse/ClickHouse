-- Tags: no-fasttest
-- no-fasttest: `COLLATE` needs ICU, which is not available in fast test

-- { echo }
SELECT count() FROM (SELECT s FROM (SELECT arrayJoin(['a', 'A', 'b']) AS s) ORDER BY s COLLATE 'en-u-ks-level2' LIMIT 1 BY s);
SELECT count() FROM (SELECT s FROM (SELECT arrayJoin(['a', 'a', 'A', 'A', 'b', 'b']) AS s) ORDER BY s COLLATE 'en-u-ks-level2' LIMIT -1 BY s);
SELECT s FROM (SELECT s FROM (SELECT arrayJoin(['a', 'a', 'A', 'A', 'b', 'b']) AS s) ORDER BY s COLLATE 'en-u-ks-level2' LIMIT -1 BY s) ORDER BY s;
