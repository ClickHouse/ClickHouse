-- Tags: no-fasttest
-- ^ depends on the Protobuf library.

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Tuple()) ENGINE = Memory();
INSERT INTO TABLE t0 (c0) VALUES (()), (());
INSERT INTO TABLE FUNCTION file(currentDatabase() || '.protobuf', 'Protobuf', 'c0 Tuple()') SELECT c0 FROM t0; -- { serverError NO_COLUMNS_SERIALIZED_TO_PROTOBUF_FIELDS }

DROP TABLE t0;
