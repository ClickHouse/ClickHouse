option (ENABLE_CURL "Enable curl" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CURL)
    if (USE_INTERNAL_CURL)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal curl with ENABLE_CURL=OFF")
    endif()
    return()
endif()

option (USE_INTERNAL_CURL "Use internal curl library" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_CURL)
    find_package (CURL)
    if (NOT CURL_FOUND)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system curl")
    endif()
endif()

if (NOT CURL_FOUND)
    set (USE_INTERNAL_CURL 1)
    set (CURL_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/curl")

    # find_package(CURL) compatibility for the following packages that uses
    # find_package(CURL)/include(FindCURL):
    # - mariadb-connector-c
    # - aws-s3-cmake
    # - sentry-native
    set (CURL_FOUND ON CACHE BOOL "")
    set (CURL_ROOT_DIR ${CURL_LIBRARY_DIR} CACHE PATH "")
    set (CURL_INCLUDE_DIR ${CURL_LIBRARY_DIR}/include CACHE PATH "")
    set (CURL_INCLUDE_DIRS ${CURL_LIBRARY_DIR}/include CACHE PATH "")
    set (CURL_LIBRARY curl CACHE STRING "")
    set (CURL_LIBRARIES ${CURL_LIBRARY} CACHE STRING "")
    set (CURL_VERSION_STRING 7.67.0 CACHE STRING "")
endif ()

message (STATUS "Using curl: ${CURL_INCLUDE_DIRS} : ${CURL_LIBRARIES}")
