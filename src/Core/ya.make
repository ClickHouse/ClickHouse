LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/openssl
    contrib/libs/sparsehash
)

SRCS(
    Block.cpp
    BlockInfo.cpp
    ColumnWithTypeAndName.cpp
    ExternalResultDescription.cpp
    ExternalTable.cpp
    Field.cpp
    MySQLProtocol.cpp
    NamesAndTypes.cpp
    Settings.cpp
    SettingsCollection.cpp
    BackgroundSchedulePool.cpp
)

END()
