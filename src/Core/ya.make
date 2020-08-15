# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/sparsehash
    contrib/restricted/boost/libs
)

SRCS(
    BackgroundSchedulePool.cpp
    BaseSettings.cpp
    Block.cpp
    BlockInfo.cpp
    ColumnWithTypeAndName.cpp
    ExternalResultDescription.cpp
    ExternalTable.cpp
    Field.cpp
    iostream_debug_helpers.cpp
    MySQLProtocol.cpp
    NamesAndTypes.cpp
    PostgreSQLProtocol.cpp
    Settings.cpp
    SettingsEnums.cpp
    SettingsFields.cpp
    SortDescription.cpp

)

END()
