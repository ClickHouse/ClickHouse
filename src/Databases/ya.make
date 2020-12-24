# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
)


SRCS(
    DatabaseAtomic.cpp
    DatabaseDictionary.cpp
    DatabaseFactory.cpp
    DatabaseLazy.cpp
    DatabaseMemory.cpp
    DatabaseOnDisk.cpp
    DatabaseOrdinary.cpp
    DatabaseWithDictionaries.cpp
    DatabasesCommon.cpp
    MySQL/ConnectionMySQLSettings.cpp
    MySQL/DatabaseConnectionMySQL.cpp
    MySQL/DatabaseMaterializeMySQL.cpp
    MySQL/FetchTablesColumnsList.cpp
    MySQL/MaterializeMetadata.cpp
    MySQL/MaterializeMySQLSettings.cpp
    MySQL/MaterializeMySQLSyncThread.cpp

)

END()
