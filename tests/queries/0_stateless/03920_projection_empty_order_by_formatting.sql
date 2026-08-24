-- Test that projection with empty ORDER BY formats consistently (round-trip)
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int32, PROJECTION p0 (SELECT c0 ORDER BY ())) ENGINE = MergeTree ORDER BY c0');
SELECT formatQuerySingleLine('CREATE TABLE t (c0 Int32, PROJECTION p0 (SELECT c0 ORDER BY (()))) ENGINE = MergeTree ORDER BY c0');
