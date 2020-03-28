LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    DatabasesCommon.cpp
    DatabaseMemory.cpp
    DatabaseOnDisk.cpp
    DatabaseOrdinary.cpp
    DatabaseWithDictionaries.cpp
    DatabaseFactory.cpp
    DatabaseLazy.cpp
    DatabaseDictionary.cpp
)

END()
