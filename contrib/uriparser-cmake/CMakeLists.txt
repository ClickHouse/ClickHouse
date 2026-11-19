set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/uriparser")

set(SRCS
    "${LIBRARY_DIR}/src/UriCommon.c"
    "${LIBRARY_DIR}/src/UriCompare.c"
    "${LIBRARY_DIR}/src/UriCopy.c"
    "${LIBRARY_DIR}/src/UriEscape.c"
    "${LIBRARY_DIR}/src/UriFile.c"
    "${LIBRARY_DIR}/src/UriIp4.c"
    "${LIBRARY_DIR}/src/UriIp4Base.c"
    "${LIBRARY_DIR}/src/UriMemory.c"
    "${LIBRARY_DIR}/src/UriNormalize.c"
    "${LIBRARY_DIR}/src/UriNormalizeBase.c"
    "${LIBRARY_DIR}/src/UriParse.c"
    "${LIBRARY_DIR}/src/UriParseBase.c"
    "${LIBRARY_DIR}/src/UriQuery.c"
    "${LIBRARY_DIR}/src/UriRecompose.c"
    "${LIBRARY_DIR}/src/UriResolve.c"
    "${LIBRARY_DIR}/src/UriSetFragment.c"
    "${LIBRARY_DIR}/src/UriSetHostAuto.c"
    "${LIBRARY_DIR}/src/UriSetHostCommon.c"
    "${LIBRARY_DIR}/src/UriSetHostIp4.c"
    "${LIBRARY_DIR}/src/UriSetHostIp6.c"
    "${LIBRARY_DIR}/src/UriSetHostIpFuture.c"
    "${LIBRARY_DIR}/src/UriSetHostRegName.c"
    "${LIBRARY_DIR}/src/UriSetPath.c"
    "${LIBRARY_DIR}/src/UriSetPort.c"
    "${LIBRARY_DIR}/src/UriSetQuery.c"
    "${LIBRARY_DIR}/src/UriSetScheme.c"
    "${LIBRARY_DIR}/src/UriSetUserInfo.c"
    "${LIBRARY_DIR}/src/UriShorten.c"
    "${LIBRARY_DIR}/src/UriVersion.c"
)

add_library(_uriparser ${SRCS})
target_compile_definitions(_uriparser PUBLIC URI_STATIC_BUILD URI_NO_UNICODE PRIVATE URI_LIBRARY_BUILD)
target_include_directories(_uriparser SYSTEM PUBLIC "${LIBRARY_DIR}/include")
target_include_directories(_uriparser SYSTEM PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/uriparser-cmake")
add_library(ch_contrib::uriparser ALIAS _uriparser)
