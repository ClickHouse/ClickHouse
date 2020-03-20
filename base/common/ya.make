LIBRARY()

ADDINCL(
    contrib/libs/cctz/include
    util/digest
)

PEERDIR(
    contrib/libs/cctz/src
    contrib/libs/poco/Net
    contrib/libs/poco/Util
    contrib/restricted/boost
    util
)

CFLAGS (-D ARCADIA_BUILD)

IF (OS_DARWIN)
    CFLAGS (-D OS_DARWIN)
ELSEIF (OS_FREEBSD)
    CFLAGS (-D OS_FREEBSD)
ELSEIF (OS_LINUX)
    CFLAGS (-D OS_LINUX)
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
