INSERT INTO TABLE FUNCTION file('foo.csv', 'CSV', 'id Int32, val Int32') PARTITION BY val VALUES (1, 1), (2, 2); -- { serverError 48 }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/test_{_partition_id}.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, '\r\n'); -- { serverError 6 }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/test_{_partition_id}.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, 'abc\x00abc'); -- { serverError 6 }
