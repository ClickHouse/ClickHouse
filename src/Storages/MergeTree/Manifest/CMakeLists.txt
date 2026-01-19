add_headers_and_sources(dbms
    IManifestStorage.h
    ManifestStorageFactory.h
    ManifestStorageFactory.cpp
)

if (TARGET ch_contrib::rocksdb)
    add_subdirectory(proto)
    add_subdirectory(RocksDB)
endif()