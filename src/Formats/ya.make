# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/protobuf
    contrib/libs/protoc
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
    registerFormats.cpp
    verbosePrintString.cpp

)

END()
