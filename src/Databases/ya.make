# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
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
    DatabaseMySQL.cpp
    DatabaseOnDisk.cpp
    DatabaseOrdinary.cpp
    DatabasesCommon.cpp
    DatabaseWithDictionaries.cpp

)

END()
