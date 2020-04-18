LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/sparsehash
    contrib/restricted/boost/libs
)

SRCS(
    BackgroundSchedulePool.cpp
    Block.cpp
    BlockInfo.cpp
    ColumnWithTypeAndName.cpp
    ExternalResultDescription.cpp
    ExternalTable.cpp
    Field.cpp
    iostream_debug_helpers.cpp
    MySQLProtocol.cpp
    NamesAndTypes.cpp
    Settings.cpp
    SettingsCollection.cpp
)

END()
