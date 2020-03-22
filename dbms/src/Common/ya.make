LIBRARY()

PEERDIR(
    clickhouse/base/common
    contrib/libs/libcpuid
    contrib/libs/re2
)

# TODO: stub for config_version.h
CFLAGS (GLOBAL -DDBMS_NAME=\"ClickHouse\")
CFLAGS (GLOBAL -DDBMS_VERSION_MAJOR=0)
CFLAGS (GLOBAL -DDBMS_VERSION_MINOR=0)
CFLAGS (GLOBAL -DDBMS_VERSION_PATCH=0)
CFLAGS (GLOBAL -DVERSION_FULL=\"Clickhouse\")
CFLAGS (GLOBAL -DVERSION_INTEGER=0)
CFLAGS (GLOBAL -DVERSION_NAME=\"Clickhouse\")
CFLAGS (GLOBAL -DVERSION_OFFICIAL=\"\\\(arcadia\\\)\")
CFLAGS (GLOBAL -DVERSION_REVISION=0)
CFLAGS (GLOBAL -DVERSION_STRING=\"Unknown\")

SRCS(
    AlignedBuffer.cpp
    Config/ConfigProcessor.cpp
    Config/ConfigReloader.cpp
    CurrentMetrics.cpp
    CurrentThread.cpp
    DNSResolver.cpp
    Dwarf.cpp
    Elf.cpp
    ErrorCodes.cpp
    escapeForFileName.cpp
    Exception.cpp
    FieldVisitors.cpp
    filesystemHelpers.cpp
    formatIPv6.cpp
    formatReadable.cpp
    getExecutablePath.cpp
    getMultipleKeysFromConfig.cpp
    getNumberOfPhysicalCPUCores.cpp
    hasLinuxCapability.cpp
    hex.cpp
    Macros.cpp
    MemoryTracker.cpp
    PipeFDs.cpp
    PODArray.cpp
    ProfileEvents.cpp
    randomSeed.cpp
    QueryProfiler.cpp
    quoteString.cpp
    SensitiveDataMasker.cpp
    setThreadName.cpp
    StackTrace.cpp
    StatusFile.cpp
    SymbolIndex.cpp
    TaskStatsInfoGetter.cpp
    ThreadFuzzer.cpp
    thread_local_rng.cpp
    ThreadPool.cpp
    ThreadStatus.cpp
    TraceCollector.cpp
    ZooKeeper/IKeeper.cpp
    ZooKeeper/TestKeeper.cpp
    ZooKeeper/ZooKeeper.cpp
    ZooKeeper/ZooKeeperNodeCache.cpp
)

END()
