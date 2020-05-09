LIBRARY()

ADDINCL (
    GLOBAL clickhouse/src
    contrib/libs/libcpuid
    contrib/libs/libunwind/include
    GLOBAL contrib/restricted/ryu
)

PEERDIR(
    clickhouse/base/common
    clickhouse/base/pcg-random
    clickhouse/base/widechar_width
    contrib/libs/libcpuid/libcpuid
    contrib/libs/openssl
    contrib/libs/poco/NetSSL_OpenSSL
    contrib/libs/re2
    contrib/restricted/ryu
)

# TODO: stub for config_version.h
CFLAGS (GLOBAL -DDBMS_NAME=\"ClickHouse\")
CFLAGS (GLOBAL -DDBMS_VERSION_MAJOR=0)
CFLAGS (GLOBAL -DDBMS_VERSION_MINOR=0)
CFLAGS (GLOBAL -DDBMS_VERSION_PATCH=0)
CFLAGS (GLOBAL -DVERSION_FULL=\"ClickHouse\")
CFLAGS (GLOBAL -DVERSION_INTEGER=0)
CFLAGS (GLOBAL -DVERSION_NAME=\"ClickHouse\")
CFLAGS (GLOBAL -DVERSION_OFFICIAL=\"\\\(arcadia\\\)\")
CFLAGS (GLOBAL -DVERSION_REVISION=0)
CFLAGS (GLOBAL -DVERSION_STRING=\"Unknown\")

SRCS(
    ActionLock.cpp
    AlignedBuffer.cpp
    checkStackSize.cpp
    ClickHouseRevision.cpp
    Config/AbstractConfigurationComparison.cpp
    Config/ConfigProcessor.cpp
    Config/configReadClient.cpp
    Config/ConfigReloader.cpp
    createHardLink.cpp
    CurrentMetrics.cpp
    CurrentThread.cpp
    DNSResolver.cpp
    Dwarf.cpp
    Elf.cpp
    ErrorCodes.cpp
    escapeForFileName.cpp
    Exception.cpp
    ExternalLoaderStatus.cpp
    FieldVisitors.cpp
    FileChecker.cpp
    filesystemHelpers.cpp
    formatIPv6.cpp
    formatReadable.cpp
    getExecutablePath.cpp
    getMultipleKeysFromConfig.cpp
    getNumberOfPhysicalCPUCores.cpp
    hasLinuxCapability.cpp
    hex.cpp
    IntervalKind.cpp
    IPv6ToBinary.cpp
    isLocalAddress.cpp
    Macros.cpp
    malloc.cpp
    MemoryStatisticsOS.cpp
    MemoryTracker.cpp
    new_delete.cpp
    OptimizedRegularExpression.cpp
    parseAddress.cpp
    parseGlobs.cpp
    parseRemoteDescription.cpp
    PipeFDs.cpp
    PODArray.cpp
    ProcfsMetricsProvider.cpp
    ProfileEvents.cpp
    QueryProfiler.cpp
    quoteString.cpp
    randomSeed.cpp
    RemoteHostFilter.cpp
    renameat2.cpp
    RWLock.cpp
    SensitiveDataMasker.cpp
    setThreadName.cpp
    SharedLibrary.cpp
    ShellCommand.cpp
    StackTrace.cpp
    StatusFile.cpp
    StatusInfo.cpp
    Stopwatch.cpp
    StringUtils/StringUtils.cpp
    StudentTTest.cpp
    SymbolIndex.cpp
    TaskStatsInfoGetter.cpp
    TerminalSize.cpp
    thread_local_rng.cpp
    ThreadFuzzer.cpp
    ThreadProfileEvents.cpp
    ThreadPool.cpp
    ThreadStatus.cpp
    TraceCollector.cpp
    UTF8Helpers.cpp
    WeakHash.cpp
    ZooKeeper/IKeeper.cpp
    ZooKeeper/Lock.cpp
    ZooKeeper/TestKeeper.cpp
    ZooKeeper/ZooKeeper.cpp
    ZooKeeper/ZooKeeperHolder.cpp
    ZooKeeper/ZooKeeperImpl.cpp
    ZooKeeper/ZooKeeperNodeCache.cpp
)

END()
