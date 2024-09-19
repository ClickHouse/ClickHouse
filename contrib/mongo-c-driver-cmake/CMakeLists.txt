option(USE_MONGODB "Enable MongoDB support" ${ENABLE_LIBRARIES})
if(NOT USE_MONGODB)
    message(STATUS "Not using libmongoc and libbson")
    return()
endif()

set(libbson_VERSION_MAJOR 1)
set(libbson_VERSION_MINOR 27)
set(libbson_VERSION_PATCH 0)
set(libbson_VERSION 1.27.0)
set(libmongoc_VERSION_MAJOR 1)
set(libmongoc_VERSION_MINOR 27)
set(libmongoc_VERSION_PATCH 0)
set(libmongoc_VERSION 1.27.0)

set(LIBBSON_SOURCES_ROOT "${ClickHouse_SOURCE_DIR}/contrib/mongo-c-driver/src")
set(LIBBSON_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/libbson/src")
file(GLOB_RECURSE LIBBSON_SOURCES "${LIBBSON_SOURCE_DIR}/*.c")

include(TestBigEndian)
test_big_endian(BSON_BIG_ENDIAN)
if(BSON_BIG_ENDIAN)
    set(BSON_BYTE_ORDER 4321)
else()
    set(BSON_BYTE_ORDER 1234)
endif()

set(BSON_OS 1)
set(BSON_EXTRA_ALIGN 1)
set(BSON_HAVE_SNPRINTF 1)
set(BSON_HAVE_TIMESPEC 1)
set(BSON_HAVE_GMTIME_R 1)
set(BSON_HAVE_RAND_R 1)
set(BSON_HAVE_STRINGS_H 1)
set(BSON_HAVE_STRLCPY 0)
set(BSON_HAVE_STRNLEN 1)
set(BSON_HAVE_STDBOOL_H 1)
set(BSON_HAVE_CLOCK_GETTIME 1)


# common settings
set(MONGOC_TRACE 0)
set(MONGOC_ENABLE_STATIC_BUILD 1)
set(MONGOC_ENABLE_DEBUG_ASSERTIONS 0)
set(MONGOC_ENABLE_MONGODB_AWS_AUTH 0)
set(MONGOC_ENABLE_SASL_CYRUS 0)
set(MONGOC_ENABLE_SASL 0)
set(MONGOC_ENABLE_SASL_SSPI 0)
set(MONGOC_HAVE_SASL_CLIENT_DONE 0)
set(MONGOC_ENABLE_SRV 0)

# DNS
set(MONGOC_HAVE_DNSAPI 0)
set(MONGOC_HAVE_RES_SEARCH 0)
set(MONGOC_HAVE_RES_NSEARCH 0)
set(MONGOC_HAVE_RES_NCLOSE 0)
set(MONGOC_HAVE_RES_NDESTROY 0)

set(MONGOC_ENABLE_COMPRESSION 1)
set(MONGOC_ENABLE_COMPRESSION_ZLIB 0)
set(MONGOC_ENABLE_COMPRESSION_SNAPPY 0)
set(MONGOC_ENABLE_COMPRESSION_ZSTD 1)

# SSL
set(MONGOC_ENABLE_CRYPTO 0)
set(MONGOC_ENABLE_CRYPTO_CNG 0)
set(MONGOC_ENABLE_CRYPTO_COMMON_CRYPTO 0)
set(MONGOC_ENABLE_CRYPTO_SYSTEM_PROFILE 0)
set(MONGOC_ENABLE_SSL 0)
set(MONGOC_ENABLE_SSL_OPENSSL 0)
set(MONGOC_ENABLE_SSL_SECURE_CHANNEL 0)
set(MONGOC_ENABLE_SSL_SECURE_TRANSPORT 0)
set(MONGOC_ENABLE_SSL_LIBRESSL 0)
set(MONGOC_ENABLE_CRYPTO_LIBCRYPTO 0)
set(MONGOC_ENABLE_CLIENT_SIDE_ENCRYPTION 0)
set(MONGOC_HAVE_ASN1_STRING_GET0_DATA 0)
if(ENABLE_SSL)
    set(MONGOC_ENABLE_SSL 1)
    set(MONGOC_ENABLE_CRYPTO 1)
    set(MONGOC_ENABLE_SSL_OPENSSL 1)
    set(MONGOC_ENABLE_CRYPTO_LIBCRYPTO 1)
    set(MONGOC_HAVE_ASN1_STRING_GET0_DATA 1)
else()
    message(WARNING "Building mongoc without SSL")
endif()

set(CMAKE_EXTRA_INCLUDE_FILES "sys/socket.h")
set(MONGOC_SOCKET_ARG2 "struct sockaddr")
set(MONGOC_HAVE_SOCKLEN 1)
set(MONGOC_SOCKET_ARG3 "socklen_t")

set(MONGOC_ENABLE_RDTSCP 0)
set(MONGOC_NO_AUTOMATIC_GLOBALS 1)
set(MONGOC_ENABLE_STATIC_INSTALL 0)
set(MONGOC_ENABLE_SHM_COUNTERS 0)
set(MONGOC_HAVE_SCHED_GETCPU 0)
set(MONGOC_HAVE_SS_FAMILY 0)

configure_file(
        ${LIBBSON_SOURCE_DIR}/bson/bson-config.h.in
        ${LIBBSON_SOURCE_DIR}/bson/bson-config.h
)
configure_file(
        ${LIBBSON_SOURCE_DIR}/bson/bson-version.h.in
        ${LIBBSON_SOURCE_DIR}/bson/bson-version.h
)

configure_file(
        ${LIBBSON_SOURCE_DIR}/bson/bson-version.h.in
        ${LIBBSON_SOURCE_DIR}/bson/bson-version.h
)

set(COMMON_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/common")
file(GLOB_RECURSE COMMON_SOURCES "${COMMON_SOURCE_DIR}/*.c")
configure_file(
        ${COMMON_SOURCE_DIR}/common-config.h.in
        ${COMMON_SOURCE_DIR}/common-config.h
)
add_library(_libbson ${LIBBSON_SOURCES} ${COMMON_SOURCES})
add_library(ch_contrib::libbson ALIAS _libbson)
target_include_directories(_libbson SYSTEM PUBLIC ${LIBBSON_SOURCE_DIR} ${COMMON_SOURCE_DIR})
target_compile_definitions(_libbson PRIVATE BSON_COMPILATION)
if(OS_LINUX)
    target_compile_definitions(_libbson PRIVATE -D_GNU_SOURCE -D_POSIX_C_SOURCE=199309L -D_XOPEN_SOURCE=600)
elseif(OS_DARWIN)
    target_compile_definitions(_libbson PRIVATE -D_DARWIN_C_SOURCE)
endif()


set(LIBMONGOC_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/libmongoc/src")
file(GLOB_RECURSE LIBMONGOC_SOURCES "${LIBMONGOC_SOURCE_DIR}/*.c")
set(UTF8PROC_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/utf8proc-2.8.0")
set(UTF8PROC_SOURCES "${UTF8PROC_SOURCE_DIR}/utf8proc.c")
set(UTHASH_SOURCE_DIR "${LIBBSON_SOURCES_ROOT}/uthash")

configure_file(
        ${LIBMONGOC_SOURCE_DIR}/mongoc/mongoc-config.h.in
        ${LIBMONGOC_SOURCE_DIR}/mongoc/mongoc-config.h
)
configure_file(
        ${LIBMONGOC_SOURCE_DIR}/mongoc/mongoc-version.h.in
        ${LIBMONGOC_SOURCE_DIR}/mongoc/mongoc-version.h
)
add_library(_libmongoc ${LIBMONGOC_SOURCES} ${COMMON_SOURCES} ${UTF8PROC_SOURCES})
add_library(ch_contrib::libmongoc ALIAS _libmongoc)
target_include_directories(_libmongoc SYSTEM PUBLIC ${LIBMONGOC_SOURCE_DIR} ${COMMON_SOURCE_DIR} ${UTF8PROC_SOURCE_DIR} ${UTHASH_SOURCE_DIR})
target_include_directories(_libmongoc SYSTEM PRIVATE ${LIBMONGOC_SOURCE_DIR}/mongoc ${UTHASH_SOURCE_DIR})
target_compile_definitions(_libmongoc PRIVATE MONGOC_COMPILATION)
target_link_libraries(_libmongoc ch_contrib::libbson ch_contrib::c-ares ch_contrib::zstd)
if(ENABLE_SSL)
    target_link_libraries(_libmongoc OpenSSL::SSL)
endif()
