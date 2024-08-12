set(NGHTTP2_DIR "${ClickHouse_SOURCE_DIR}/contrib/nghttp2/lib")

set(NGHTTP2_SRC
    "${NGHTTP2_DIR}/nghttp2_pq.c"
    "${NGHTTP2_DIR}/nghttp2_map.c"
    "${NGHTTP2_DIR}/nghttp2_queue.c"
    "${NGHTTP2_DIR}/nghttp2_frame.c"
    "${NGHTTP2_DIR}/nghttp2_buf.c"
    "${NGHTTP2_DIR}/nghttp2_stream.c"
    "${NGHTTP2_DIR}/nghttp2_outbound_item.c"
    "${NGHTTP2_DIR}/nghttp2_session.c"
    "${NGHTTP2_DIR}/nghttp2_submit.c"
    "${NGHTTP2_DIR}/nghttp2_helper.c"
    "${NGHTTP2_DIR}/nghttp2_alpn.c"
    "${NGHTTP2_DIR}/nghttp2_hd.c"
    "${NGHTTP2_DIR}/nghttp2_hd_huffman.c"
    "${NGHTTP2_DIR}/nghttp2_hd_huffman_data.c"
    "${NGHTTP2_DIR}/nghttp2_version.c"
    "${NGHTTP2_DIR}/nghttp2_priority_spec.c"
    "${NGHTTP2_DIR}/nghttp2_option.c"
    "${NGHTTP2_DIR}/nghttp2_callbacks.c"
    "${NGHTTP2_DIR}/nghttp2_mem.c"
    "${NGHTTP2_DIR}/nghttp2_http.c"
    "${NGHTTP2_DIR}/nghttp2_rcbuf.c"
    "${NGHTTP2_DIR}/nghttp2_extpri.c"
    "${NGHTTP2_DIR}/nghttp2_ratelim.c"
    "${NGHTTP2_DIR}/nghttp2_time.c"
    "${NGHTTP2_DIR}/nghttp2_debug.c"
    "${NGHTTP2_DIR}/sfparse.c"
)

set(NGHTTP2_INCLUDES
    "${ClickHouse_SOURCE_DIR}/contrib/nghttp2/lib/includes"
    "${ClickHouse_SOURCE_DIR}/contrib/nghttp2-cmake/includes"
    "${ClickHouse_BINARY_DIR}/contrib/nghttp2-cmake"
)

include(CheckIncludeFile)
check_include_file("arpa/inet.h"    HAVE_ARPA_INET_H)
check_include_file("fcntl.h"        HAVE_FCNTL_H)
check_include_file("inttypes.h"     HAVE_INTTYPES_H)
check_include_file("limits.h"       HAVE_LIMITS_H)
check_include_file("netdb.h"        HAVE_NETDB_H)
check_include_file("netinet/in.h"   HAVE_NETINET_IN_H)
check_include_file("netinet/ip.h"   HAVE_NETINET_IP_H)
check_include_file("pwd.h"          HAVE_PWD_H)
check_include_file("sys/socket.h"   HAVE_SYS_SOCKET_H)
check_include_file("sys/time.h"     HAVE_SYS_TIME_H)
check_include_file("syslog.h"       HAVE_SYSLOG_H)
check_include_file("unistd.h"       HAVE_UNISTD_H)
check_include_file("windows.h"      HAVE_WINDOWS_H)

add_definitions(-DHAVE_CONFIG_H)
configure_file("${ClickHouse_SOURCE_DIR}/contrib/nghttp2/cmakeconfig.h.in" "${ClickHouse_BINARY_DIR}/contrib/nghttp2-cmake/config.h")

add_library(_nghttp2 ${NGHTTP2_SRC})

target_include_directories(_nghttp2 SYSTEM BEFORE PUBLIC ${NGHTTP2_INCLUDES})

add_library(ch_contrib::nghttp2 ALIAS _nghttp2)