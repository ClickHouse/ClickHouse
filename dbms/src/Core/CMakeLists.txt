if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

set (SRCS
    AccurateComparison.h
    BackgroundSchedulePool.cpp
    BackgroundSchedulePool.h
    Block.cpp
    Block.h
    BlockInfo.cpp
    BlockInfo.h
    callOnTypeIndex.h
    CMakeLists.txt
    ColumnNumbers.h
    ColumnsWithTypeAndName.h
    ColumnWithTypeAndName.cpp
    ColumnWithTypeAndName.h
    config_core.h.in
    DecimalComparison.h
    Defines.h
    ExternalResultDescription.cpp
    ExternalResultDescription.h
    ExternalTable.cpp
    ExternalTable.h
    Field.cpp
    Field.h
    iostream_debug_helpers.cpp
    iostream_debug_helpers.h
    MySQLProtocol.cpp
    MySQLProtocol.h
    NamesAndTypes.cpp
    NamesAndTypes.h
    Names.h
    Protocol.h
    QualifiedTableName.h
    QueryProcessingStage.h
    Row.h
    SettingsCommon.cpp
    SettingsCommon.h
    Settings.cpp
    Settings.h
    SortCursor.h
    SortDescription.h
    TypeListNumber.h
    Types.h
    UUID.h
)

add_library(clickhouse_core ${SRCS})
