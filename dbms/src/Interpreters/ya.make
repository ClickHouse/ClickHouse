LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    AsynchronousMetrics.cpp
    castColumn.cpp
    ClientInfo.cpp
    Context.cpp
    convertFieldToType.cpp
    DatabaseCatalog.cpp
    DDLWorker.cpp
    DNSCacheUpdater.cpp
    executeQuery.cpp
    ExternalLoader.cpp
    ExternalLoaderXMLConfigRepository.cpp
    IExternalLoadable.cpp
    InternalTextLogsQueue.cpp
    InterpreterCreateQuotaQuery.cpp
    InterpreterCreateRoleQuery.cpp
    InterpreterCreateRowPolicyQuery.cpp
    InterpreterCreateUserQuery.cpp
    InterpreterGrantQuery.cpp
    InterpreterShowCreateAccessEntityQuery.cpp
    InterpreterShowGrantsQuery.cpp
    loadMetadata.cpp
    ProcessList.cpp
    StorageID.cpp
    TablesStatus.cpp
    ThreadStatusExt.cpp
)

END()
