-- Exercise ParserSnapshotQuery::parseImpl and the nested helpers
-- (parseElement, parseExceptDatabases, parseExceptTables, parseSnapshotDestination)
-- by round-tripping a variety of SNAPSHOT statements through formatQuery.

SELECT '--- element: TABLE ---';
SELECT formatQuery('SNAPSHOT TABLE db.tbl TO S3(\'s3://bucket/snap/\')');
SELECT formatQuery('SNAPSHOT TABLE tbl TO S3(\'s3://bucket/snap/\')');

SELECT '--- element: ALL ---';
SELECT formatQuery('SNAPSHOT ALL TO S3(\'s3://bucket/snap/\')');

SELECT '--- element: ALL + EXCEPT DATABASE (singular) ---';
SELECT formatQuery('SNAPSHOT ALL EXCEPT DATABASE d1 TO S3(\'s3://bucket/snap/\')');

SELECT '--- element: ALL + EXCEPT DATABASES (plural, list) ---';
SELECT formatQuery('SNAPSHOT ALL EXCEPT DATABASES d1, d2, d3 TO S3(\'s3://bucket/snap/\')');

SELECT '--- element: ALL + EXCEPT TABLE (singular) ---';
SELECT formatQuery('SNAPSHOT ALL EXCEPT TABLE x.a TO S3(\'s3://bucket/snap/\')');

SELECT '--- element: ALL + EXCEPT TABLES (plural, list) ---';
SELECT formatQuery('SNAPSHOT ALL EXCEPT TABLES x.a, y.b, z.c TO S3(\'s3://bucket/snap/\')');

SELECT '--- element: ALL + EXCEPT DATABASES + EXCEPT TABLES ---';
SELECT formatQuery('SNAPSHOT ALL EXCEPT DATABASES d1, d2 EXCEPT TABLES t1.a, t2.b TO S3(\'s3://bucket/snap/\')');

SELECT '--- destination: Disk ---';
SELECT formatQuery('SNAPSHOT ALL TO Disk(\'default\', \'/snap/\')');

SELECT '--- destination: File ---';
SELECT formatQuery('SNAPSHOT TABLE db.tbl TO File(\'/tmp/snap/\')');

SELECT '--- destination: bare identifier (no parameters) ---';
SELECT formatQuery('SNAPSHOT ALL TO Null');

SELECT '--- error: missing element (no TABLE/ALL keyword) ---';
SELECT formatQuery('SNAPSHOT TO S3(\'s3://b/\')'); -- { serverError SYNTAX_ERROR }

SELECT '--- error: DATABASE keyword is not accepted (only TABLE or ALL) ---';
SELECT formatQuery('SNAPSHOT DATABASE mydb TO S3(\'s3://b/\')'); -- { serverError SYNTAX_ERROR }

SELECT '--- error: missing TO keyword ---';
SELECT formatQuery('SNAPSHOT ALL S3(\'s3://b/\')'); -- { serverError SYNTAX_ERROR }

SELECT '--- error: missing destination after TO ---';
SELECT formatQuery('SNAPSHOT ALL TO'); -- { serverError SYNTAX_ERROR }

SELECT '--- error: EXCEPT DATABASE without a database name ---';
SELECT formatQuery('SNAPSHOT ALL EXCEPT DATABASE TO S3(\'s3://b/\')'); -- { serverError SYNTAX_ERROR }

SELECT '--- error: EXCEPT TABLES without any tables ---';
SELECT formatQuery('SNAPSHOT ALL EXCEPT TABLES TO S3(\'s3://b/\')'); -- { serverError SYNTAX_ERROR }

SELECT '--- error: TABLE without table name ---';
SELECT formatQuery('SNAPSHOT TABLE TO S3(\'s3://b/\')'); -- { serverError SYNTAX_ERROR }
