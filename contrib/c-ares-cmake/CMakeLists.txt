set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/c-ares")

# Generated from contrib/c-ares/src/lib/Makefile.inc
SET(SRCS
    "${LIBRARY_DIR}/src/lib/ares__addrinfo2hostent.c"
    "${LIBRARY_DIR}/src/lib/ares__addrinfo_localhost.c"
    "${LIBRARY_DIR}/src/lib/ares__close_sockets.c"
    "${LIBRARY_DIR}/src/lib/ares__get_hostent.c"
    "${LIBRARY_DIR}/src/lib/ares__parse_into_addrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares__readaddrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares__sortaddrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares__read_line.c"
    "${LIBRARY_DIR}/src/lib/ares__timeval.c"
    "${LIBRARY_DIR}/src/lib/ares_android.c"
    "${LIBRARY_DIR}/src/lib/ares_cancel.c"
    "${LIBRARY_DIR}/src/lib/ares_data.c"
    "${LIBRARY_DIR}/src/lib/ares_destroy.c"
    "${LIBRARY_DIR}/src/lib/ares_expand_name.c"
    "${LIBRARY_DIR}/src/lib/ares_expand_string.c"
    "${LIBRARY_DIR}/src/lib/ares_fds.c"
    "${LIBRARY_DIR}/src/lib/ares_free_hostent.c"
    "${LIBRARY_DIR}/src/lib/ares_free_string.c"
    "${LIBRARY_DIR}/src/lib/ares_freeaddrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_getaddrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_getenv.c"
    "${LIBRARY_DIR}/src/lib/ares_gethostbyaddr.c"
    "${LIBRARY_DIR}/src/lib/ares_gethostbyname.c"
    "${LIBRARY_DIR}/src/lib/ares_getnameinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_getsock.c"
    "${LIBRARY_DIR}/src/lib/ares_init.c"
    "${LIBRARY_DIR}/src/lib/ares_library_init.c"
    "${LIBRARY_DIR}/src/lib/ares_llist.c"
    "${LIBRARY_DIR}/src/lib/ares_mkquery.c"
    "${LIBRARY_DIR}/src/lib/ares_create_query.c"
    "${LIBRARY_DIR}/src/lib/ares_nowarn.c"
    "${LIBRARY_DIR}/src/lib/ares_options.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_a_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_aaaa_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_caa_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_mx_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_naptr_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_ns_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_ptr_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_soa_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_srv_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_txt_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_uri_reply.c"
    "${LIBRARY_DIR}/src/lib/ares_platform.c"
    "${LIBRARY_DIR}/src/lib/ares_process.c"
    "${LIBRARY_DIR}/src/lib/ares_query.c"
    "${LIBRARY_DIR}/src/lib/ares_rand.c"
    "${LIBRARY_DIR}/src/lib/ares_search.c"
    "${LIBRARY_DIR}/src/lib/ares_send.c"
    "${LIBRARY_DIR}/src/lib/ares_strcasecmp.c"
    "${LIBRARY_DIR}/src/lib/ares_strdup.c"
    "${LIBRARY_DIR}/src/lib/ares_strerror.c"
    "${LIBRARY_DIR}/src/lib/ares_strsplit.c"
    "${LIBRARY_DIR}/src/lib/ares_timeout.c"
    "${LIBRARY_DIR}/src/lib/ares_version.c"
    "${LIBRARY_DIR}/src/lib/ares_writev.c"
    "${LIBRARY_DIR}/src/lib/bitncmp.c"
    "${LIBRARY_DIR}/src/lib/inet_net_pton.c"
    "${LIBRARY_DIR}/src/lib/inet_ntop.c"
    "${LIBRARY_DIR}/src/lib/windows_port.c"
)

add_library(_c-ares STATIC ${SRCS})
target_compile_definitions(_c-ares PUBLIC CARES_STATICLIB)

target_compile_definitions(_c-ares PRIVATE HAVE_CONFIG_H=1)

target_include_directories(_c-ares SYSTEM PUBLIC
    "${LIBRARY_DIR}/src/lib"
    "${LIBRARY_DIR}/include"
)

# Platform-specific include directories. The original build system does a lot of checks to eventually generate two header files with defines:
# ares_build.h and ares_config.h. To update, run the original CMake build in c-ares for each platform and copy the headers into the
# platform-specific folder.
# For the platform-specific compile definitions, see c-ares top-level CMakeLists.txt.
if (OS_LINUX)
    target_include_directories(_c-ares SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/c-ares-cmake/linux")
    target_compile_definitions(_c-ares PRIVATE -D_GNU_SOURCE -D_POSIX_C_SOURCE=199309L -D_XOPEN_SOURCE=600)
elseif (OS_DARWIN)
    target_include_directories(_c-ares SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/c-ares-cmake/darwin")
    target_compile_definitions(_c-ares PRIVATE -D_DARWIN_C_SOURCE)
elseif (OS_FREEBSD)
    target_include_directories(_c-ares SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/c-ares-cmake/freebsd")
elseif (OS_SUNOS)
    target_include_directories(_c-ares SYSTEM PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/c-ares-cmake/solaris")
endif()

add_library(ch_contrib::c-ares ALIAS _c-ares)
