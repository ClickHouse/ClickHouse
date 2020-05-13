LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/protobuf_std
)

SRCS(
    FormatFactory.cpp
    FormatSchemaInfo.cpp
    IRowInputStream.cpp
    IRowOutputStream.cpp
    JSONEachRowUtils.cpp
    MySQLBlockInputStream.cpp
    NativeFormat.cpp
    NullFormat.cpp
    ParsedTemplateFormatString.cpp
    ProtobufColumnMatcher.cpp
    ProtobufReader.cpp
    ProtobufSchemas.cpp
    ProtobufWriter.cpp
    verbosePrintString.cpp
)

END()
