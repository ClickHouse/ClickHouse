# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
)


SRCS(
    DiskCacheWrapper.cpp
    DiskDecorator.cpp
    DiskFactory.cpp
    DiskLocal.cpp
    DiskMemory.cpp
    DiskSelector.cpp
    IDisk.cpp
    IVolume.cpp
    SingleDiskVolume.cpp
    StoragePolicy.cpp
    VolumeJBOD.cpp
    VolumeRAID1.cpp
    createVolume.cpp
    registerDisks.cpp

)

END()
