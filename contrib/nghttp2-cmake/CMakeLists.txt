set(NGHTTP2_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/nghttp2")
set(NGHTTP2_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Use the same source list as in nghttp2/lib/CMakeLists.txt
set(NGHTTP2_SOURCES
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_pq.c 
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_map.c 
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_queue.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_frame.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_buf.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_stream.c 
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_outbound_item.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_session.c 
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_submit.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_helper.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_alpn.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_hd.c 
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_hd_huffman.c 
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_hd_huffman_data.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_version.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_priority_spec.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_option.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_callbacks.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_mem.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_http.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_rcbuf.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_extpri.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_ratelim.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_time.c
  ${NGHTTP2_SOURCE_DIR}/lib/nghttp2_debug.c
  ${NGHTTP2_SOURCE_DIR}/lib/sfparse.c
)

# Set configuration values for cmakeconfig.h
set(HINT_NORETURN "__attribute__((noreturn))")
set(DEBUGBUILD 0)
set(HAVE_ARPA_INET_H 1)
set(HAVE_NETINET_IN_H 1)
set(HAVE_UNISTD_H 1)
set(HAVE_SYS_TIME_H 1)
set(HAVE_SYS_SOCKET_H 1)
set(HAVE_NETDB_H 1)
set(HAVE_FCNTL_H 1)
set(HAVE_LIMITS_H 1)

configure_file(
    ${NGHTTP2_SOURCE_DIR}/cmakeconfig.h.in
    ${NGHTTP2_BINARY_DIR}/cmakeconfig.h
    @ONLY
)

add_library(nghttp2_static ${NGHTTP2_SOURCES})

target_include_directories(nghttp2_static SYSTEM BEFORE PUBLIC 
    ${NGHTTP2_SOURCE_DIR}/lib/includes
    ${NGHTTP2_SOURCE_DIR}/lib
    ${NGHTTP2_BINARY_DIR}
)

target_compile_definitions(nghttp2_static PUBLIC 
    NGHTTP2_STATICLIB
)

target_compile_definitions(nghttp2_static PRIVATE 
    HAVE_CONFIG_H
    BUILDING_NGHTTP2
)

add_library(ch_contrib::nghttp2 ALIAS nghttp2_static)
