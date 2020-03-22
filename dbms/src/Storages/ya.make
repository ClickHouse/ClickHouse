LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
    contrib/libs/sparsehash
)

SRCS(
    IStorage.cpp
    registerStorages.cpp
    System/attachSystemTables.cpp
)

END()
