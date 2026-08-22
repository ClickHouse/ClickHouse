-- Tags: no-fasttest

SELECT name, is_output, is_tty_friendly FROM system.formats WHERE name IN ('Pretty', 'TSV', 'JSON', 'JSONEachRow',
'ODBCDriver2', 'Parquet', 'Arrow', 'BSONEachRow', 'Protobuf', 'ProtobufList', 'ProtobufSingle', 'CapnProto', 'Npy', 'ArrowStream', 'ORC', 'MsgPack', 'Avro', 'RowBinary', 'RowBinaryWithNames', 'RowBinaryWithNamesAndTypes', 'Native', 'Buffers', 'MySQLWire', 'PostgreSQLWire')
ORDER BY name;
