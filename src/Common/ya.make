# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
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

INCLUDE(${ARCADIA_ROOT}/clickhouse/cmake/yandex/ya.make.versions.inc)

CFLAGS(-g0)

SRCS(
    ActionLock.cpp
    AlignedBuffer.cpp
    Allocator.cpp
    checkStackSize.cpp
    clearPasswordFromCommandLine.cpp
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
    getMappedArea.cpp
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
    OpenSSLHelpers.cpp
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
    remapExecutable.cpp
    RemoteHostFilter.cpp
    renameat2.cpp
    RWLock.cpp
    SensitiveDataMasker.cpp
    setThreadName.cpp
    SettingsChanges.cpp
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
    ThreadFuzzer.cpp
    thread_local_rng.cpp
    ThreadPool.cpp
    ThreadProfileEvents.cpp
    ThreadStatus.cpp
    TraceCollector.cpp
    UnicodeBar.cpp
    UTF8Helpers.cpp
    WeakHash.cpp
    ZooKeeper/IKeeper.cpp
    ZooKeeper/TestKeeper.cpp
    ZooKeeper/ZooKeeper.cpp
    ZooKeeper/ZooKeeperImpl.cpp
    ZooKeeper/ZooKeeperNodeCache.cpp

)

END()
