set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/c-ares")

# Generated from contrib/c-ares/src/lib/Makefile.inc
SET(SRCS
    "${LIBRARY_DIR}/src/lib/ares_addrinfo2hostent.c"
    "${LIBRARY_DIR}/src/lib/ares_addrinfo_localhost.c"
    "${LIBRARY_DIR}/src/lib/ares_android.c"
    "${LIBRARY_DIR}/src/lib/ares_cancel.c"
    "${LIBRARY_DIR}/src/lib/ares_close_sockets.c"
    "${LIBRARY_DIR}/src/lib/ares_conn.c"
    "${LIBRARY_DIR}/src/lib/ares_cookie.c"
    "${LIBRARY_DIR}/src/lib/ares_data.c"
    "${LIBRARY_DIR}/src/lib/ares_destroy.c"
    "${LIBRARY_DIR}/src/lib/ares_free_hostent.c"
    "${LIBRARY_DIR}/src/lib/ares_free_string.c"
    "${LIBRARY_DIR}/src/lib/ares_freeaddrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_getaddrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_gethostbyaddr.c"
    "${LIBRARY_DIR}/src/lib/ares_gethostbyname.c"
    "${LIBRARY_DIR}/src/lib/ares_getnameinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_hosts_file.c"
    "${LIBRARY_DIR}/src/lib/ares_init.c"
    "${LIBRARY_DIR}/src/lib/ares_library_init.c"
    "${LIBRARY_DIR}/src/lib/ares_metrics.c"
    "${LIBRARY_DIR}/src/lib/ares_options.c"
    "${LIBRARY_DIR}/src/lib/ares_parse_into_addrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_process.c"
    "${LIBRARY_DIR}/src/lib/ares_qcache.c"
    "${LIBRARY_DIR}/src/lib/ares_query.c"
    "${LIBRARY_DIR}/src/lib/ares_search.c"
    "${LIBRARY_DIR}/src/lib/ares_send.c"
    "${LIBRARY_DIR}/src/lib/ares_set_socket_functions.c"
    "${LIBRARY_DIR}/src/lib/ares_socket.c"
    "${LIBRARY_DIR}/src/lib/ares_sortaddrinfo.c"
    "${LIBRARY_DIR}/src/lib/ares_strerror.c"
    "${LIBRARY_DIR}/src/lib/ares_sysconfig.c"
    "${LIBRARY_DIR}/src/lib/ares_sysconfig_files.c"
    "${LIBRARY_DIR}/src/lib/ares_sysconfig_mac.c"
    "${LIBRARY_DIR}/src/lib/ares_timeout.c"
    "${LIBRARY_DIR}/src/lib/ares_update_servers.c"
    "${LIBRARY_DIR}/src/lib/ares_version.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_array.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_htable.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_htable_asvp.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_htable_dict.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_htable_strvp.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_htable_szvp.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_htable_vpstr.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_htable_vpvp.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_llist.c"
    "${LIBRARY_DIR}/src/lib/dsa/ares_slist.c"
    "${LIBRARY_DIR}/src/lib/event/ares_event_configchg.c"
    "${LIBRARY_DIR}/src/lib/event/ares_event_epoll.c"
    "${LIBRARY_DIR}/src/lib/event/ares_event_kqueue.c"
    "${LIBRARY_DIR}/src/lib/event/ares_event_poll.c"
    "${LIBRARY_DIR}/src/lib/event/ares_event_select.c"
    "${LIBRARY_DIR}/src/lib/event/ares_event_thread.c"
    "${LIBRARY_DIR}/src/lib/event/ares_event_wake_pipe.c"
    "${LIBRARY_DIR}/src/lib/inet_net_pton.c"
    "${LIBRARY_DIR}/src/lib/inet_ntop.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_create_query.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_expand_name.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_expand_string.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_fds.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_getsock.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_a_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_aaaa_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_caa_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_mx_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_naptr_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_ns_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_ptr_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_soa_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_srv_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_txt_reply.c"
    "${LIBRARY_DIR}/src/lib/legacy/ares_parse_uri_reply.c"
    "${LIBRARY_DIR}/src/lib/record/ares_dns_mapping.c"
    "${LIBRARY_DIR}/src/lib/record/ares_dns_multistring.c"
    "${LIBRARY_DIR}/src/lib/record/ares_dns_name.c"
    "${LIBRARY_DIR}/src/lib/record/ares_dns_parse.c"
    "${LIBRARY_DIR}/src/lib/record/ares_dns_record.c"
    "${LIBRARY_DIR}/src/lib/record/ares_dns_write.c"
    "${LIBRARY_DIR}/src/lib/str/ares_buf.c"
    "${LIBRARY_DIR}/src/lib/str/ares_str.c"
    "${LIBRARY_DIR}/src/lib/str/ares_strsplit.c"
    "${LIBRARY_DIR}/src/lib/util/ares_iface_ips.c"
    "${LIBRARY_DIR}/src/lib/util/ares_math.c"
    "${LIBRARY_DIR}/src/lib/util/ares_rand.c"
    "${LIBRARY_DIR}/src/lib/util/ares_threads.c"
    "${LIBRARY_DIR}/src/lib/util/ares_timeval.c"
    "${LIBRARY_DIR}/src/lib/util/ares_uri.c"
)

add_library(_c-ares STATIC ${SRCS})
target_compile_definitions(_c-ares PUBLIC CARES_STATICLIB)

target_compile_definitions(_c-ares PRIVATE HAVE_CONFIG_H=1)

target_include_directories(_c-ares SYSTEM PUBLIC
    "${LIBRARY_DIR}/src/lib/include"
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
