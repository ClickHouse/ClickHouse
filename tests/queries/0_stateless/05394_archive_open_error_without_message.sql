-- Tags: no-fasttest
-- Tag no-fasttest: depends on libarchive and S3

-- gzip of 'x\n' without the last byte of its 8-byte trailer: libarchive fails to open it without setting an error message.
INSERT INTO FUNCTION file(currentDatabase() || '_05394.tar.gz', 'RawBLOB', 'x String', 'none')
SELECT unhex('1f8b0800000000000203abe002001f08ea46020000') SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05394.tar.gz :: x', 'CSV', 'x String'); -- { serverError CANNOT_UNPACK_ARCHIVE }

INSERT INTO FUNCTION s3('http://localhost:11111/test/05394_' || currentDatabase() || '.tar.gz', 'test', 'testtest', 'RawBLOB', 'x String', 'none')
SELECT unhex('1f8b0800000000000203abe002001f08ea46020000') SETTINGS s3_truncate_on_insert = 1;
SELECT * FROM s3('http://localhost:11111/test/05394_' || currentDatabase() || '.tar.gz :: x', 'test', 'testtest', 'CSV', 'x String'); -- { serverError CANNOT_UNPACK_ARCHIVE }
