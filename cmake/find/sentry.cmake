set (SENTRY_LIBRARY "sentry")
set (SENTRY_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/sentry-native/include")
if (NOT EXISTS "${SENTRY_INCLUDE_DIR}/sentry.h")
    message (WARNING "submodule contrib/sentry-native is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif ()

option (USE_SENTRY "Use Sentry" ON)

set (BUILD_SHARED_LIBS OFF)
set (SENTRY_PIC OFF)
set (SENTRY_BACKEND "none")
set (SENTRY_TRANSPORT "curl")
set (CURL_LIBRARY ${ClickHouse_SOURCE_DIR}/contrib/curl/lib)
set (CURL_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/curl/include)

message (STATUS "Using sentry=${USE_SENTRY}: ${SENTRY_LIBRARY}")

include_directories("${SENTRY_INCLUDE_DIR}")