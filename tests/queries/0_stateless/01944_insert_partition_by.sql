-- Tags: no-fasttest
-- Tag no-fasttest: needs s3

INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/test_{_partition_id}.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, '\r\n'); -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/test_{_partition_id}.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, 'abc\x00abc'); -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/test_{_partition_id}.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, 'abc\xc3\x28abc'); -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/test_{_partition_id}.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, 'abc}{abc'); -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/test_{_partition_id}.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, 'abc*abc'); -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/foo/{_partition_id}', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, ''); -- { serverError BAD_ARGUMENTS }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/{_partition_id}/key.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, ''); -- { serverError BAD_ARGUMENTS }
INSERT INTO TABLE FUNCTION s3('http://localhost:9001/{_partition_id}/key.csv', 'admin', 'admin', 'CSV', 'id Int32, val String') PARTITION BY val VALUES (1, 'aa/bb'); -- { serverError CANNOT_PARSE_TEXT }
