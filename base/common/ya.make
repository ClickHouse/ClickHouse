LIBRARY()

ADDINCL(
    # public
    GLOBAL clickhouse/base
    GLOBAL clickhouse/dbms/src
    GLOBAL util/digest # for <city.h>

    # private
    contrib/libs/cctz/include
)

PEERDIR(
    contrib/libs/cctz/src
    contrib/libs/cxxsupp/libcxx-filesystem
    contrib/libs/poco/Net
    contrib/libs/poco/Util
    contrib/restricted/boost
    util
)

CFLAGS (GLOBAL -DARCADIA_BUILD)

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
    StringRef.cpp
    terminalColors.cpp
)

END()
