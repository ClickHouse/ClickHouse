# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
LIBRARY()

PEERDIR(
    clickhouse/src/Common
    contrib/libs/sparsehash
    contrib/restricted/boost/libs
)

CFLAGS(-g0)

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
    MySQL/Authentication.cpp
    MySQL/IMySQLReadPacket.cpp
    MySQL/IMySQLWritePacket.cpp
    MySQL/MySQLClient.cpp
    MySQL/MySQLGtid.cpp
    MySQL/MySQLReplication.cpp
    MySQL/PacketEndpoint.cpp
    MySQL/PacketsConnection.cpp
    MySQL/PacketsGeneric.cpp
    MySQL/PacketsProtocolText.cpp
    MySQL/PacketsReplication.cpp
    NamesAndTypes.cpp
    PostgreSQLProtocol.cpp
    Settings.cpp
    SettingsEnums.cpp
    SettingsFields.cpp
    SortDescription.cpp

)

END()
