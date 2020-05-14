LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    DiskFactory.cpp
    DiskLocal.cpp
    DiskHDFS.cpp
    DiskMemory.cpp
    DiskSelector.cpp
    IDisk.cpp
    IVolume.cpp
    registerDisks.cpp
    StoragePolicy.cpp
    VolumeJBOD.cpp
)

END()
