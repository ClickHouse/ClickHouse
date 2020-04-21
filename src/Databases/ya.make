LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    DatabaseDictionary.cpp
    DatabaseFactory.cpp
    DatabaseLazy.cpp
    DatabaseMemory.cpp
    DatabaseMySQL.cpp
    DatabaseOnDisk.cpp
    DatabaseOrdinary.cpp
    DatabasesCommon.cpp
    DatabaseWithDictionaries.cpp
)

END()
