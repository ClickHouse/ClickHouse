# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

ADDINCL(
    GLOBAL clickhouse/base
)

CFLAGS (GLOBAL -DARCADIA_BUILD)

CFLAGS (GLOBAL -DUSE_CPUID=1)
CFLAGS (GLOBAL -DUSE_JEMALLOC=0)
CFLAGS (GLOBAL -DUSE_RAPIDJSON=1)
CFLAGS (GLOBAL -DUSE_SSL=1)

IF (OS_DARWIN)
    CFLAGS (GLOBAL -DOS_DARWIN)
ELSEIF (OS_FREEBSD)
    CFLAGS (GLOBAL -DOS_FREEBSD)
ELSEIF (OS_LINUX)
    CFLAGS (GLOBAL -DOS_LINUX)
ENDIF ()

PEERDIR(
    contrib/libs/cctz
    contrib/libs/cxxsupp/libcxx-filesystem
    contrib/libs/poco/Net
    contrib/libs/poco/Util
    contrib/libs/poco/NetSSL_OpenSSL
    contrib/libs/fmt
    contrib/restricted/boost
    contrib/restricted/cityhash-1.0.2
)

CFLAGS(-g0)

SRCS(
    DateLUT.cpp
    DateLUTImpl.cpp
    JSON.cpp
    LineReader.cpp
    StringRef.cpp
    argsToConfig.cpp
    coverage.cpp
    demangle.cpp
    errnoToString.cpp
    getFQDNOrHostName.cpp
    getMemoryAmount.cpp
    getPageSize.cpp
    getResource.cpp
    getThreadId.cpp
    mremap.cpp
    phdr_cache.cpp
    preciseExp10.cpp
    setTerminalEcho.cpp
    shift10.cpp
    sleep.cpp
    terminalColors.cpp

)

END()
