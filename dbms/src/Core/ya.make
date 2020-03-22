LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    Block.cpp
    BlockInfo.cpp
    ColumnWithTypeAndName.cpp
    ExternalTable.cpp
    Field.cpp
    MySQLProtocol.cpp
    SettingsCollection.cpp
)

END()
