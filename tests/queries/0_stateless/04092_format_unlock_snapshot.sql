SELECT formatQuery('ALTER TABLE t UNLOCK SNAPSHOT ''snap1'' FROM S3(''http://example.com'')');
SELECT formatQuery('SYSTEM UNLOCK SNAPSHOT ''snap1'' FROM S3(''http://example.com'')');

