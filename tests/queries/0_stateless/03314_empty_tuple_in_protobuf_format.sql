-- Tag: no-fasttest

DROP TABLE IF EXISTS t0;

SET engine_file_truncate_on_insert = 1;
CREATE TABLE t0 (c0 Tuple()) ENGINE = Memory();
INSERT INTO TABLE t0 (c0) VALUES (()), (());
INSERT INTO TABLE FUNCTION file('/tmp/test_03314.proto', 'Protobuf', 'c0 Tuple()') SELECT c0 FROM t0; -- serverError { NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS }

DROP TABLE t0;
