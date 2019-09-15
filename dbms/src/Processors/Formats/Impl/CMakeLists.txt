if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

set (SRCS
    ArrowColumnToCHColumn.cpp
    ArrowColumnToCHColumn.h
    BinaryRowInputFormat.cpp
    BinaryRowInputFormat.h
    BinaryRowOutputFormat.cpp
    BinaryRowOutputFormat.h
    CapnProtoRowInputFormat.cpp
    CapnProtoRowInputFormat.h
    CMakeLists.txt
    CSVRowInputFormat.cpp
    CSVRowInputFormat.h
    CSVRowOutputFormat.cpp
    CSVRowOutputFormat.h
    JSONCompactRowOutputFormat.cpp
    JSONCompactRowOutputFormat.h
    JSONEachRowRowInputFormat.cpp
    JSONEachRowRowInputFormat.h
    JSONEachRowRowOutputFormat.cpp
    JSONEachRowRowOutputFormat.h
    JSONEachRowWithProgressRowOutputFormat.cpp
    JSONEachRowWithProgressRowOutputFormat.h
    JSONRowOutputFormat.cpp
    JSONRowOutputFormat.h
    MySQLOutputFormat.cpp
    MySQLOutputFormat.h
    NativeFormat.cpp
    NullFormat.cpp
    ODBCDriver2BlockOutputFormat.cpp
    ODBCDriver2BlockOutputFormat.h
    ODBCDriverBlockOutputFormat.cpp
    ODBCDriverBlockOutputFormat.h
    ORCBlockInputFormat.cpp
    ORCBlockInputFormat.h
    ParquetBlockInputFormat.cpp
    ParquetBlockInputFormat.h
    ParquetBlockOutputFormat.cpp
    ParquetBlockOutputFormat.h
    PrettyBlockOutputFormat.cpp
    PrettyBlockOutputFormat.h
    PrettyCompactBlockOutputFormat.cpp
    PrettyCompactBlockOutputFormat.h
    PrettySpaceBlockOutputFormat.cpp
    PrettySpaceBlockOutputFormat.h
    ProtobufRowInputFormat.cpp
    ProtobufRowInputFormat.h
    ProtobufRowOutputFormat.cpp
    ProtobufRowOutputFormat.h
    TabSeparatedRawRowOutputFormat.h
    TabSeparatedRowInputFormat.cpp
    TabSeparatedRowInputFormat.h
    TabSeparatedRowOutputFormat.cpp
    TabSeparatedRowOutputFormat.h
    TemplateBlockOutputFormat.cpp
    TemplateBlockOutputFormat.h
    TemplateRowInputFormat.cpp
    TemplateRowInputFormat.h
    TSKVRowInputFormat.cpp
    TSKVRowInputFormat.h
    TSKVRowOutputFormat.cpp
    TSKVRowOutputFormat.h
    ValuesRowInputFormat.cpp
    ValuesRowInputFormat.h
    ValuesRowOutputFormat.cpp
    ValuesRowOutputFormat.h
    VerticalRowOutputFormat.cpp
    VerticalRowOutputFormat.h
    XMLRowOutputFormat.cpp
    XMLRowOutputFormat.h
)

add_library(clickhouse_processors_formats_impl ${SRCS})
