# Build description for libpq which is part of the PostgreSQL sources

set(POSTGRES_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/postgres")
set(LIBPQ_SOURCE_DIR "${POSTGRES_SOURCE_DIR}/src/interfaces/libpq")
set(LIBPQ_CMAKE_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/postgres-cmake")

set(SRCS
    "${LIBPQ_SOURCE_DIR}/fe-auth.c"
    "${LIBPQ_SOURCE_DIR}/fe-auth-scram.c"
    "${LIBPQ_SOURCE_DIR}/fe-connect.c"
    "${LIBPQ_SOURCE_DIR}/fe-exec.c"
    "${LIBPQ_SOURCE_DIR}/fe-lobj.c"
    "${LIBPQ_SOURCE_DIR}/fe-misc.c"
    "${LIBPQ_SOURCE_DIR}/fe-print.c"
    "${LIBPQ_SOURCE_DIR}/fe-trace.c"
    "${LIBPQ_SOURCE_DIR}/fe-protocol3.c"
    "${LIBPQ_SOURCE_DIR}/fe-secure.c"
    "${LIBPQ_SOURCE_DIR}/fe-secure-common.c"
    "${LIBPQ_SOURCE_DIR}/fe-secure-openssl.c"
    "${LIBPQ_SOURCE_DIR}/legacy-pqsignal.c"
    "${LIBPQ_SOURCE_DIR}/libpq-events.c"
    "${LIBPQ_SOURCE_DIR}/pqexpbuffer.c"

    "${POSTGRES_SOURCE_DIR}/src/common/scram-common.c"
    "${POSTGRES_SOURCE_DIR}/src/common/sha2.c"
    "${POSTGRES_SOURCE_DIR}/src/common/sha1.c"
    "${POSTGRES_SOURCE_DIR}/src/common/md5.c"
    "${POSTGRES_SOURCE_DIR}/src/common/md5_common.c"
    "${POSTGRES_SOURCE_DIR}/src/common/hmac_openssl.c"
    "${POSTGRES_SOURCE_DIR}/src/common/cryptohash.c"
    "${POSTGRES_SOURCE_DIR}/src/common/saslprep.c"
    "${POSTGRES_SOURCE_DIR}/src/common/unicode_norm.c"
    "${POSTGRES_SOURCE_DIR}/src/common/ip.c"
    "${POSTGRES_SOURCE_DIR}/src/common/jsonapi.c"
    "${POSTGRES_SOURCE_DIR}/src/common/wchar.c"
    "${POSTGRES_SOURCE_DIR}/src/common/base64.c"
    "${POSTGRES_SOURCE_DIR}/src/common/link-canary.c"
    "${POSTGRES_SOURCE_DIR}/src/common/fe_memutils.c"
    "${POSTGRES_SOURCE_DIR}/src/common/string.c"
    "${POSTGRES_SOURCE_DIR}/src/common/pg_get_line.c"
    "${POSTGRES_SOURCE_DIR}/src/common/pg_prng.c"
    "${POSTGRES_SOURCE_DIR}/src/common/stringinfo.c"
    "${POSTGRES_SOURCE_DIR}/src/common/psprintf.c"
    "${POSTGRES_SOURCE_DIR}/src/common/encnames.c"
    "${POSTGRES_SOURCE_DIR}/src/common/logging.c"

    "${POSTGRES_SOURCE_DIR}/src/port/snprintf.c"
    "${POSTGRES_SOURCE_DIR}/src/port/strlcat.c"
    "${POSTGRES_SOURCE_DIR}/src/port/strlcpy.c"
    "${POSTGRES_SOURCE_DIR}/src/port/strerror.c"
    "${POSTGRES_SOURCE_DIR}/src/port/inet_net_ntop.c"
    "${POSTGRES_SOURCE_DIR}/src/port/getpeereid.c"
    "${POSTGRES_SOURCE_DIR}/src/port/chklocale.c"
    "${POSTGRES_SOURCE_DIR}/src/port/noblock.c"
    "${POSTGRES_SOURCE_DIR}/src/port/pg_strong_random.c"
    "${POSTGRES_SOURCE_DIR}/src/port/pgstrcasecmp.c"
    "${POSTGRES_SOURCE_DIR}/src/port/pg_bitutils.c"
    "${POSTGRES_SOURCE_DIR}/src/port/thread.c"
    "${POSTGRES_SOURCE_DIR}/src/port/path.c"
)

add_library(_libpq ${SRCS})

add_definitions(-DHAVE_BIO_METH_NEW)
add_definitions(-DHAVE_HMAC_CTX_NEW)
add_definitions(-DHAVE_HMAC_CTX_FREE)

target_include_directories (_libpq SYSTEM PUBLIC ${LIBPQ_SOURCE_DIR})
target_include_directories (_libpq SYSTEM PUBLIC "${POSTGRES_SOURCE_DIR}/src/include")
target_include_directories (_libpq SYSTEM PUBLIC "${LIBPQ_CMAKE_SOURCE_DIR}") # pre-generated headers

# NOTE: this is a dirty hack to avoid and instead pg_config.h should be shipped
# for different OS'es like for jemalloc, not one generic for all OS'es like
# now.
if (OS_DARWIN OR OS_FREEBSD OR USE_MUSL)
    target_compile_definitions(_libpq PRIVATE -DSTRERROR_R_INT=1)
endif()

target_link_libraries (_libpq PRIVATE OpenSSL::SSL)

add_library(ch_contrib::libpq ALIAS _libpq)
