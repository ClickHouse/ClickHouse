LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    DiskFactory.cpp
    DiskLocal.cpp
    DiskMemory.cpp
    DiskSpaceMonitor.cpp
    IDisk.cpp
    registerDisks.cpp
)

END()
