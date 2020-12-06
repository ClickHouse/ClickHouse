# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

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


SRCS(
    ActionLock.cpp
    AlignedBuffer.cpp
    Allocator.cpp
    ClickHouseRevision.cpp
    Config/AbstractConfigurationComparison.cpp
    Config/ConfigProcessor.cpp
    Config/ConfigReloader.cpp
    Config/configReadClient.cpp
    CurrentMetrics.cpp
    CurrentThread.cpp
    DNSResolver.cpp
    Dwarf.cpp
    Elf.cpp
    ErrorCodes.cpp
    Exception.cpp
    ExternalLoaderStatus.cpp
    FieldVisitors.cpp
    FileChecker.cpp
    IPv6ToBinary.cpp
    IntervalKind.cpp
    Macros.cpp
    MemoryStatisticsOS.cpp
    MemoryTracker.cpp
    OpenSSLHelpers.cpp
    OptimizedRegularExpression.cpp
    PODArray.cpp
    PipeFDs.cpp
    ProcfsMetricsProvider.cpp
    ProfileEvents.cpp
    QueryProfiler.cpp
    RWLock.cpp
    RemoteHostFilter.cpp
    SensitiveDataMasker.cpp
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
    ThreadPool.cpp
    ThreadProfileEvents.cpp
    ThreadStatus.cpp
    TraceCollector.cpp
    UTF8Helpers.cpp
    UnicodeBar.cpp
    WeakHash.cpp
    ZooKeeper/IKeeper.cpp
    ZooKeeper/TestKeeper.cpp
    ZooKeeper/ZooKeeper.cpp
    ZooKeeper/ZooKeeperImpl.cpp
    ZooKeeper/ZooKeeperNodeCache.cpp
    checkStackSize.cpp
    clearPasswordFromCommandLine.cpp
    createHardLink.cpp
    escapeForFileName.cpp
    filesystemHelpers.cpp
    formatIPv6.cpp
    formatReadable.cpp
    getExecutablePath.cpp
    getMappedArea.cpp
    getMultipleKeysFromConfig.cpp
    getNumberOfPhysicalCPUCores.cpp
    hasLinuxCapability.cpp
    hex.cpp
    isLocalAddress.cpp
    malloc.cpp
    new_delete.cpp
    parseAddress.cpp
    parseGlobs.cpp
    parseRemoteDescription.cpp
    quoteString.cpp
    randomSeed.cpp
    remapExecutable.cpp
    renameat2.cpp
    setThreadName.cpp
    thread_local_rng.cpp

)

END()
