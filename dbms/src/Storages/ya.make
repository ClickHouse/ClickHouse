LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
    contrib/libs/sparsehash
)

SRCS(
    ColumnsDescription.cpp
    IStorage.cpp
    registerStorages.cpp
    StorageReplicatedMergeTree.cpp
    System/attachSystemTables.cpp
)

END()
