LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    DiskFactory.cpp
    DiskLocal.cpp
    DiskMemory.cpp
    DiskSelector.cpp
    IDisk.cpp
    IVolume.cpp
    registerDisks.cpp
    StoragePolicy.cpp
    VolumeJBOD.cpp
)

END()
