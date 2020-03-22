LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    AsynchronousMetrics.cpp
    Context.cpp
    DatabaseCatalog.cpp
    DDLWorker.cpp
    DNSCacheUpdater.cpp
    ExternalLoader.cpp
    ExternalLoaderXMLConfigRepository.cpp
    loadMetadata.cpp
    ProcessList.cpp
)

END()
