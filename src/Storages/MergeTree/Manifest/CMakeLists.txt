add_headers_and_sources(dbms
    IManifestStorage.h
    ManifestStorageFactory.h
    ManifestStorageFactory.cpp
    BaseManifestEntry.h
    BaseManifestEntry.cpp
)

if (TARGET ch_contrib::rocksdb)
    add_subdirectory(RocksDB)
endif()
