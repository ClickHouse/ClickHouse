set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/sentry-native")

set (SRCS
    ${SRC_DIR}/vendor/mpack.c
    ${SRC_DIR}/src/sentry_alloc.c
    ${SRC_DIR}/src/sentry_backend.c
    ${SRC_DIR}/src/sentry_core.c
    ${SRC_DIR}/src/sentry_database.c
    ${SRC_DIR}/src/sentry_envelope.c
    ${SRC_DIR}/src/sentry_json.c
    ${SRC_DIR}/src/sentry_logger.c
    ${SRC_DIR}/src/sentry_options.c
    ${SRC_DIR}/src/sentry_random.c
    ${SRC_DIR}/src/sentry_ratelimiter.c
    ${SRC_DIR}/src/sentry_scope.c
    ${SRC_DIR}/src/sentry_session.c
    ${SRC_DIR}/src/sentry_slice.c
    ${SRC_DIR}/src/sentry_string.c
    ${SRC_DIR}/src/sentry_sync.c
    ${SRC_DIR}/src/sentry_transport.c
    ${SRC_DIR}/src/sentry_utils.c
    ${SRC_DIR}/src/sentry_uuid.c
    ${SRC_DIR}/src/sentry_value.c
    ${SRC_DIR}/src/path/sentry_path.c
    ${SRC_DIR}/src/transports/sentry_disk_transport.c
    ${SRC_DIR}/src/transports/sentry_function_transport.c
    ${SRC_DIR}/src/unwinder/sentry_unwinder.c
    ${SRC_DIR}/src/sentry_unix_pageallocator.c
    ${SRC_DIR}/src/path/sentry_path_unix.c
    ${SRC_DIR}/src/symbolizer/sentry_symbolizer_unix.c
    ${SRC_DIR}/src/transports/sentry_transport_curl.c
    ${SRC_DIR}/src/backends/sentry_backend_none.c
)

if(APPLE)
    list(APPEND SRCS ${SRC_DIR}/src/modulefinder/sentry_modulefinder_apple.c)
else()
    list(APPEND SRCS ${SRC_DIR}/src/modulefinder/sentry_modulefinder_linux.c)
endif()

add_library(sentry ${SRCS})
add_library(sentry::sentry ALIAS sentry)

if(BUILD_SHARED_LIBS)
    target_compile_definitions(sentry PRIVATE SENTRY_BUILD_SHARED)
else()
    target_compile_definitions(sentry PUBLIC SENTRY_BUILD_STATIC)
endif()

target_link_libraries(sentry PRIVATE curl pthread)
target_include_directories(sentry PUBLIC "${SRC_DIR}/include" PRIVATE "${SRC_DIR}/src")
target_compile_definitions(sentry PRIVATE SENTRY_WITH_INPROC_BACKEND SIZEOF_LONG=8)
