LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    AsynchronousMetrics.cpp
    ClientInfo.cpp
    Context.cpp
    convertFieldToType.cpp
    DatabaseCatalog.cpp
    DDLWorker.cpp
    DNSCacheUpdater.cpp
    executeQuery.cpp
    ExternalLoader.cpp
    ExternalLoaderXMLConfigRepository.cpp
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
    TablesStatus.cpp
    ThreadStatusExt.cpp
)

END()
