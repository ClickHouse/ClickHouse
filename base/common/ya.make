LIBRARY()

ADDINCL(
    # public
    GLOBAL clickhouse/base
    GLOBAL clickhouse/contrib/cityhash
    GLOBAL clickhouse/dbms/src

    # private
    contrib/libs/cctz/include
)

PEERDIR(
    contrib/libs/cctz/src
    contrib/libs/cxxsupp/libcxx-filesystem
    contrib/libs/poco/Net
    contrib/libs/poco/Util
    contrib/restricted/boost/libs
)

CFLAGS(
    GLOBAL -DARCADIA_BUILD
)

CXXFLAGS(
    GLOBAL -std=c++20
)

IF (OS_DARWIN)
    CFLAGS (GLOBAL -DOS_DARWIN)
ELSEIF (OS_FREEBSD)
    CFLAGS (GLOBAL -DOS_FREEBSD)
ELSEIF (OS_LINUX)
    CFLAGS (GLOBAL -DOS_LINUX)
ENDIF ()

SRCS(
    argsToConfig.cpp
    coverage.cpp
    DateLUT.cpp
    DateLUTImpl.cpp
    demangle.cpp
    getFQDNOrHostName.cpp
    getMemoryAmount.cpp
    getThreadId.cpp
    JSON.cpp
    LineReader.cpp
    mremap.cpp
    phdr_cache.cpp
    preciseExp10.c
    setTerminalEcho.cpp
    shift10.cpp
    sleep.cpp
    terminalColors.cpp
)

END()
