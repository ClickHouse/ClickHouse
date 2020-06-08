LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    createVolume.cpp
    DiskFactory.cpp
    DiskLocal.cpp
    DiskMemory.cpp
    DiskSelector.cpp
    IDisk.cpp
    IVolume.cpp
    registerDisks.cpp
    SingleDiskVolume.cpp
    StoragePolicy.cpp
    VolumeJBOD.cpp
)

END()
