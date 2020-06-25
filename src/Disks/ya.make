LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    DiskFactory.cpp
    DiskLocal.cpp
    DiskMemory.cpp
    DiskS3.cpp
    DiskSpaceMonitor.cpp
    IDisk.cpp
    registerDisks.cpp
)

END()
