cmake_minimum_required(VERSION 3.5.1)


set(GRPC_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/grpc)
set(GRPC_INCLUDE_DIR ${GRPC_SOURCE_DIR}/include/)
set(GRPC_BINARY_DIR ${ClickHouse_BINARY_DIR}/contrib/grpc)


if(UNIX)
  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(_gRPC_PLATFORM_LINUX ON)
  elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(_gRPC_PLATFORM_MAC ON)
  else()
    set(_gRPC_PLATFORM_POSIX ON)
  endif()
endif()

if(_gRPC_PLATFORM_MAC)
  set(_gRPC_ALLTARGETS_LIBRARIES ${CMAKE_DL_LIBS} m pthread)
elseif(UNIX)
  set(_gRPC_ALLTARGETS_LIBRARIES ${CMAKE_DL_LIBS} rt m pthread)
endif()


# address_sorting.cmake
set(_gRPC_ADDRESS_SORTING_INCLUDE_DIR "${GRPC_SOURCE_DIR}/third_party/address_sorting/include")
set(_gRPC_ADDRESS_SORTING_LIBRARIES address_sorting)

# cares.cmake
set(CARES_ROOT_DIR ${GRPC_SOURCE_DIR}/third_party/cares/cares)
set(CARES_BINARY_DIR ${GRPC_BINARY_DIR}/third_party/cares/cares)
set(CARES_SHARED ${BUILD_SHARED_LIBS} CACHE BOOL "" FORCE)
if(BUILD_SHARED_LIBS)
  set(CARES_STATIC OFF CACHE BOOL "" FORCE)
else()
  set(CARES_STATIC ON CACHE BOOL "" FORCE)
endif()
set(_gRPC_CARES_LIBRARIES c-ares)
add_subdirectory(${CARES_ROOT_DIR} ${CARES_BINARY_DIR})

# upb.cmake
set(UPB_ROOT_DIR ${GRPC_SOURCE_DIR}/third_party/upb)
set(_gRPC_UPB_INCLUDE_DIR "${UPB_ROOT_DIR}")
set(_gRPC_UPB_GRPC_GENERATED_DIR "${GRPC_SOURCE_DIR}/src/core/ext/upb-generated")
set(_gRPC_UPB_LIBRARIES upb)

# protobuf.cmake
set(_gRPC_PROTOBUF_INCLUDE_DIR ${Protobuf_INCLUDE_DIR})
set(_gRPC_PROTOBUF_LIBRARIES ${Protobuf_LIBRARY})
set(_gRPC_PROTOBUF_PROTOC_LIBRARIES ${Protobuf_PROTOC_LIBRARY})
set(_gRPC_PROTOBUF_PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})

# ssl.cmake
set(_gRPC_SSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR})
set(_gRPC_SSL_LIBRARIES ${OPENSSL_SSL_LIBRARY})

# zlib.cmake
set(_gRPC_ZLIB_INCLUDE_DIR ${ZLIB_INCLUDE_DIR})
set(_gRPC_ZLIB_LIBRARIES ${ZLIB_LIBRARIES})


add_library(address_sorting
  ${GRPC_SOURCE_DIR}/third_party/address_sorting/address_sorting.c
  ${GRPC_SOURCE_DIR}/third_party/address_sorting/address_sorting_posix.c
  ${GRPC_SOURCE_DIR}/third_party/address_sorting/address_sorting_windows.c
)

target_include_directories(address_sorting
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)
target_link_libraries(address_sorting
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
)


add_library(gpr
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/alloc.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/atm.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_iphone.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/cpu_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/env_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/env_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/env_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_android.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/log_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/murmur_hash.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string_util_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/string_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/sync.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/sync_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/sync_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time_precise.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/time_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tls_pthread.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tmpfile_msys.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tmpfile_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/tmpfile_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gpr/wrap_memcpy.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/arena.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/fork.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/global_config_env.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/host_port.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/mpscq.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/thd_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/gprpp/thd_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/profiling/basic_timers.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/profiling/stap_timers.cc
)

target_include_directories(gpr
  PUBLIC ${GRPC_INCLUDE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(gpr
  ${_gRPC_ALLTARGETS_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
)

add_library(grpc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/avl/avl.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/backoff/backoff.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack_builder.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/connected_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/status_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/message_compress.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_gzip.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_identity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats_data.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/format_request.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/buffer_list.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/call_combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/cfstream_handle.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epoll1_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epollex_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_poll_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/exec_ctx.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/mpmcqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/threadpool.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_host_name_max.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_sysconf.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_unsupported.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/internal_errqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iocp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/is_epollexclusive_available.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/load_file.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/lockfree_event.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/polling_entity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resource_quota.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/sockaddr_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_factory_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_mutator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_common_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_common.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_ifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_noifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/time_averaged_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_heap.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_manager.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/udp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix_noop.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_eventfd.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_nospecial.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_pipe.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_writer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/b64.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/percent_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_intern.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_string_helpers.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/api_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_details.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_log_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_stack_type.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/event_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/lame_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/metadata_array.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/validate_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/version.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/bdp_estimator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/byte_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/connectivity_state.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/error_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/pid_controller.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/static_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_conversion.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/timeout_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport_op_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/uri/uri_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/trace.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/secure/server_secure_chttp2.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_decoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/context_list.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/flow_control.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_data.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_goaway.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_rst_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_window_update.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_table.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/http2_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/huffsyms.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/incoming_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_lists.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_map.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/varint.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/writing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/alpn/alpn.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client/http_client_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/http_filters_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/message_compress/message_compress_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/server/http_server_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/context/security_context.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/alts_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/composite/composite_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/credentials_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/fake/fake_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/google_default/credentials_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/google_default/google_default_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/iam/iam_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/json_token.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/jwt_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/jwt/jwt_verifier.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/local/local_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/oauth2/oauth2_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/plugin/plugin_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/ssl/ssl_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/tls/grpc_tls_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/tls/spiffe_credentials.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/alts/alts_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/fake/fake_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/load_system_roots_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/load_system_roots_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/local/local_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl/ssl_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/ssl_utils_config.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/security_connector/tls/spiffe_security_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/client_auth_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/secure_endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/security_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/server_auth_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/target_authority_table.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/transport/tsi_error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/util/json_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init_secure.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/crypt/aes_gcm.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/crypt/gsec.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_counter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_frame_protector.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_record_protocol_crypter_common.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_seal_privacy_integrity_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/alts_unseal_privacy_integrity_crypter.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/frame_protector/frame_handler.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_handshaker_client.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_shared_resource.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_tsi_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_integrity_only_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_privacy_integrity_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_grpc_record_protocol_common.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_iovec_record_protocol.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/zero_copy_frame_protector/alts_zero_copy_grpc_protector.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_no_op.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/check_gcp_environment_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_client_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/security/credentials/alts/grpc_alts_credentials_server_options.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/alts_tsi_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/alts/handshaker/transport_security_common_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/altscontext.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/handshaker.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/gcp/transport_security_common.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/core/tsi/transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/authority.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/chttp2_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backend_metric.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backup_poller.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/channel_connectivity.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/global_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/health/health_check_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_connect_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_proxy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/local_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/parse_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_result_parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolving_lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/retry_throttle.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/server_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/service_config.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel_pool_interface.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/deadline/deadline_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/udpa/data/orca/v1/orca_load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/gogoproto/gogo.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/validate/validate.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/annotations.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/http.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/any.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/descriptor.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/duration.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/empty.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/struct.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/timestamp.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/wrappers.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/rpc/status.upb.c
  ${GRPC_SOURCE_DIR}/src/core/tsi/fake_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/local_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_boringssl.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_cache.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl/session_cache/ssl_session_openssl.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/ssl_transport_security.cc
  ${GRPC_SOURCE_DIR}/src/core/tsi/transport_security_grpc.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/chttp2_server.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/secure/secure_channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/client_load_reporting_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_channel_secure.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/load_balancer_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/lb/v1/load_balancer.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/fake/fake_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/xds/xds.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_bootstrap.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_channel_secure.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/auth/cert.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/circuit_breaker.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/filter.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/outlier_detection.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/discovery.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/eds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/endpoint.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/discovery/v2/ads.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/load_stats/v2/lrs.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/address.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/base.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/config_source.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/grpc_service.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/health_check.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/http_uri.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/protocol.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/percent.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/range.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/round_robin/round_robin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/dns_resolver_ares.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/dns_resolver_selection.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/native/dns_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/sockaddr/sockaddr_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/xds/xds_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/census/grpc_context.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_idle/client_idle_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/max_age/max_age_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/message_size/message_size_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client_authority_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_cronet_compression_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/plugin_registry/grpc_plugin_registry.cc
)


target_include_directories(grpc
  PUBLIC ${GRPC_INCLUDE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc
  ${_gRPC_SSL_LIBRARIES}
  ${_gRPC_ZLIB_LIBRARIES}
  ${_gRPC_CARES_LIBRARIES}
  ${_gRPC_ADDRESS_SORTING_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  gpr
)
if (_gRPC_PLATFORM_MAC)
  target_link_libraries(grpc "-framework CoreFoundation")
endif()


add_library(grpc_unsecure
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/init_unsecure.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/avl/avl.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/backoff/backoff.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_stack_builder.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channel_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/channelz_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/connected_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/handshaker_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/channel/status_util.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_args.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/compression_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/message_compress.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_gzip.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/compression/stream_compression_identity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/stats_data.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/format_request.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/httpcli.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/http/parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/buffer_list.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/call_combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/cfstream_handle.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/combiner.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/endpoint_pair_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/error_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epoll1_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_epollex_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_poll_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/ev_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/exec_ctx.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/mpmcqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/executor/threadpool.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/fork_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_host_name_max.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/gethostname_sysconf.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/grpc_if_nametoindex_unsupported.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/internal_errqueue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iocp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_internal.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_posix_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/iomgr_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/is_epollexclusive_available.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/load_file.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/lockfree_event.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/polling_entity.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_set_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/pollset_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resolve_address_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/resource_quota.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/sockaddr_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_factory_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_mutator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_common_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_linux.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_utils_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/socket_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_cfstream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_client_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_common.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_ifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_utils_posix_noifaddrs.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_server_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/tcp_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/time_averaged_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_custom.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_generic.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_heap.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_manager.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/timer_uv.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/udp_server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/unix_sockets_posix_noop.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_eventfd.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_nospecial.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_pipe.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/iomgr/wakeup_fd_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/json/json_writer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/b64.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/percent_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_intern.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/slice/slice_string_helpers.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/api_trace.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/byte_buffer_reader.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_details.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/call_log_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_init.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/channel_stack_type.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/completion_queue_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/event_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/lame_client.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/metadata_array.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/server.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/validate_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/surface/version.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/bdp_estimator.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/byte_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/connectivity_state.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/error_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/metadata_batch.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/pid_controller.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/static_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_conversion.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/status_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/timeout_encoding.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/transport/transport_op_string.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/uri/uri_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/lib/debug/trace.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/insecure/server_chttp2_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_decoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/bin_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/chttp2_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/context_list.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/flow_control.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_data.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_goaway.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_ping.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_rst_stream.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/frame_window_update.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_encoder.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_parser.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/hpack_table.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/http2_settings.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/huffsyms.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/incoming_metadata.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_lists.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/stream_map.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/varint.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/transport/writing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/alpn/alpn.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client/http_client_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/http_filters_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/message_compress/message_compress_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/server/http_server_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/server/chttp2_server.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/insecure/channel_create_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/authority.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/chttp2/client/chttp2_connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backend_metric.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/backup_poller.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/channel_connectivity.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_channelz.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_factory.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/client_channel_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/connector.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/global_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/health/health_check_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_connect_handshaker.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/http_proxy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/local_subchannel_pool.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/parse_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/proxy_mapper_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_registry.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver_result_parsing.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolving_lb_policy.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/retry_throttle.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/server_address.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/service_config.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/subchannel_pool_interface.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/deadline/deadline_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/udpa/data/orca/v1/orca_load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/gogoproto/gogo.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/validate/validate.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/annotations.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/api/http.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/any.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/descriptor.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/duration.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/empty.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/struct.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/timestamp.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/protobuf/wrappers.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/google/rpc/status.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_plugin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/transport/inproc/inproc_transport.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/dns_resolver_ares.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_fallback.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_libuv.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_posix.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_windows.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/dns_resolver_selection.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/dns/native/dns_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/sockaddr/sockaddr_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/fake/fake_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/resolver/xds/xds_resolver.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_bootstrap.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/xds/xds_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/auth/cert.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/circuit_breaker.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/filter.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/cluster/outlier_detection.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/discovery.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/eds.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/endpoint.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/endpoint/load_report.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/discovery/v2/ads.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/service/load_stats/v2/lrs.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/address.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/base.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/config_source.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/grpc_service.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/health_check.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/http_uri.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/api/v2/core/protocol.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/percent.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/envoy/type/range.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/client_load_reporting_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_channel.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_client_stats.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/grpclb/load_balancer_api.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/lb/v1/load_balancer.upb.c
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/xds/xds.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_channel/lb_policy/round_robin/round_robin.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/census/grpc_context.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/client_idle/client_idle_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/max_age/max_age_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/message_size/message_size_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/http/client_authority_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_cronet_compression_filter.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/filters/workarounds/workaround_utils.cc
  ${GRPC_SOURCE_DIR}/src/core/plugin_registry/grpc_unsecure_plugin_registry.cc
)

target_include_directories(grpc_unsecure
  PUBLIC ${GRPC_INCLUDE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc_unsecure
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_ZLIB_LIBRARIES}
  ${_gRPC_CARES_LIBRARIES}
  ${_gRPC_ADDRESS_SORTING_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  gpr
)

if (_gRPC_PLATFORM_MAC)
  target_link_libraries(grpc_unsecure "-framework CoreFoundation")
endif()


add_library(grpc++
  ${GRPC_SOURCE_DIR}/src/cpp/client/insecure_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/secure_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/auth_property_iterator.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/secure_auth_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/secure_channel_arguments.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/secure_create_auth_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/tls_credentials_options.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/tls_credentials_options_util.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/insecure_server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/secure_server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/channel_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_interceptor.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_internal.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/credentials_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/generic_stub.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/alarm.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_arguments.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_filter.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/completion_queue_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/core_codegen.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/resource_quota_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/rpc_method.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/validate_service_config.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/version_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/async_generic_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/channel_argument_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/create_default_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/dynamic_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/external_connection_acceptor_impl.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/default_health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service_server_builder_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_builder.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/thread_manager/thread_manager.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/byte_buffer_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/status.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/string_ref.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/time_cc.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/cpp/codegen/codegen_init.cc
)

target_include_directories(grpc++
  PUBLIC ${GRPC_INCLUDE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc++
  ${_gRPC_SSL_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc
  gpr
)


add_library(grpc++_unsecure
  ${GRPC_SOURCE_DIR}/src/cpp/client/insecure_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/insecure_create_auth_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/insecure_server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/channel_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/client_interceptor.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_internal.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/create_channel_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/credentials_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/client/generic_stub.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/alarm.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_arguments.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/channel_filter.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/completion_queue_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/core_codegen.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/resource_quota_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/rpc_method.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/validate_service_config.cc
  ${GRPC_SOURCE_DIR}/src/cpp/common/version_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/async_generic_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/channel_argument_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/create_default_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/dynamic_thread_pool.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/external_connection_acceptor_impl.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/default_health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/health/health_check_service_server_builder_option.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_builder.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_context.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_credentials.cc
  ${GRPC_SOURCE_DIR}/src/cpp/server/server_posix.cc
  ${GRPC_SOURCE_DIR}/src/cpp/thread_manager/thread_manager.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/byte_buffer_cc.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/status.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/string_ref.cc
  ${GRPC_SOURCE_DIR}/src/cpp/util/time_cc.cc
  ${GRPC_SOURCE_DIR}/src/core/ext/upb-generated/src/proto/grpc/health/v1/health.upb.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/decode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/encode.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/msg.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/port.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/table.c
  ${GRPC_SOURCE_DIR}/third_party/upb/upb/upb.c
  ${GRPC_SOURCE_DIR}/src/cpp/codegen/codegen_init.cc
)

target_include_directories(grpc++_unsecure
  PUBLIC ${GRPC_INCLUDE_DIR}
  PRIVATE ${GRPC_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTO_GENS_DIR}
)

target_link_libraries(grpc++_unsecure
  ${_gRPC_BASELIB_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  gpr
  grpc_unsecure
)


if (_gRPC_SSL_INCLUDE_DIR AND _gRPC_SSL_LIBRARIES)
  add_library(libgrpc ALIAS grpc)
  add_library(libgrpc++ ALIAS grpc++)
else()
  add_library(libgrpc ALIAS grpc_unsecure)
  add_library(libgrpc++ ALIAS grpc++_unsecure)
endif()


add_library(grpc_plugin_support
  ${GRPC_SOURCE_DIR}/src/compiler/cpp_generator.cc
)

target_include_directories(grpc_plugin_support
  PRIVATE ${GRPC_SOURCE_DIR}
  PUBLIC ${GRPC_INCLUDE_DIR}
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc_plugin_support
  ${_gRPC_PROTOBUF_PROTOC_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
)


add_executable(grpc_cpp_plugin
  ${GRPC_SOURCE_DIR}/src/compiler/cpp_plugin.cc
)

target_include_directories(grpc_cpp_plugin
  PRIVATE ${GRPC_SOURCE_DIR}
  PUBLIC ${GRPC_INCLUDE_DIR}
  PRIVATE ${_gRPC_ADDRESS_SORTING_INCLUDE_DIR}
  PRIVATE ${_gRPC_CARES_INCLUDE_DIR}
  PRIVATE ${_gRPC_PROTOBUF_INCLUDE_DIR}
  PRIVATE ${_gRPC_SSL_INCLUDE_DIR}
  PRIVATE ${_gRPC_UPB_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_GRPC_GENERATED_DIR}
  PRIVATE ${_gRPC_UPB_INCLUDE_DIR}
  PRIVATE ${_gRPC_ZLIB_INCLUDE_DIR}
)

target_link_libraries(grpc_cpp_plugin
  ${_gRPC_PROTOBUF_PROTOC_LIBRARIES}
  ${_gRPC_PROTOBUF_LIBRARIES}
  ${_gRPC_ALLTARGETS_LIBRARIES}
  grpc_plugin_support
)
