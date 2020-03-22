LIBRARY()

PEERDIR(
    clickhouse/base/common
    contrib/libs/re2
)

# TODO: stub for config_version.h
CFLAGS (GLOBAL -DDBMS_NAME=\"ClickHouse\")
CFLAGS (GLOBAL -DVERSION_STRING=\"Unknown\")
CFLAGS (GLOBAL -DVERSION_OFFICIAL=\"\\\(arcadia\\\)\")
CFLAGS (GLOBAL -DVERSION_FULL=\"Clickhouse\")
CFLAGS (GLOBAL -DVERSION_REVISION=0)
CFLAGS (GLOBAL -DVERSION_INTEGER=0)
CFLAGS (GLOBAL -DDBMS_VERSION_MAJOR=0)
CFLAGS (GLOBAL -DDBMS_VERSION_MINOR=0)
CFLAGS (GLOBAL -DDBMS_VERSION_PATCH=0)

SRCS(
    ClickHouseRevision.cpp
    Config/ConfigProcessor.cpp
    Config/ConfigReloader.cpp
    CurrentMetrics.cpp
    CurrentThread.cpp
    DNSResolver.cpp
    ErrorCodes.cpp
    Exception.cpp
    formatReadable.cpp
    getMultipleKeysFromConfig.cpp
    getNumberOfPhysicalCPUCores.cpp
    Macros.cpp
    MemoryTracker.cpp
    ProfileEvents.cpp
    quoteString.cpp
    SensitiveDataMasker.cpp
    setThreadName.cpp
    StatusFile.cpp
    ThreadFuzzer.cpp
    ThreadPool.cpp
    ThreadStatus.cpp
    ZooKeeper/ZooKeeperNodeCache.cpp
)

END()
