set(ENABLE_NURAFT_DEFAULT ${ENABLE_LIBRARIES})
if (OS_FREEBSD)
    set(ENABLE_NURAFT_DEFAULT OFF)
    message (STATUS "Using internal NuRaft library on FreeBSD and Darwin is not supported")
endif()
option(ENABLE_NURAFT "Enable NuRaft" ${ENABLE_NURAFT_DEFAULT})

if (NOT ENABLE_NURAFT)
    message(STATUS "Not using NuRaft")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/NuRaft")

set(SRCS
    "${LIBRARY_DIR}/src/handle_priority.cxx"
    "${LIBRARY_DIR}/src/buffer_serializer.cxx"
    "${LIBRARY_DIR}/src/peer.cxx"
    "${LIBRARY_DIR}/src/global_mgr.cxx"
    "${LIBRARY_DIR}/src/buffer.cxx"
    "${LIBRARY_DIR}/src/asio_service.cxx"
    "${LIBRARY_DIR}/src/handle_client_request.cxx"
    "${LIBRARY_DIR}/src/raft_server.cxx"
    "${LIBRARY_DIR}/src/snapshot.cxx"
    "${LIBRARY_DIR}/src/handle_commit.cxx"
    "${LIBRARY_DIR}/src/error_code.cxx"
    "${LIBRARY_DIR}/src/crc32.cxx"
    "${LIBRARY_DIR}/src/handle_snapshot_sync.cxx"
    "${LIBRARY_DIR}/src/stat_mgr.cxx"
    "${LIBRARY_DIR}/src/handle_join_leave.cxx"
    "${LIBRARY_DIR}/src/handle_user_cmd.cxx"
    "${LIBRARY_DIR}/src/handle_custom_notification.cxx"
    "${LIBRARY_DIR}/src/handle_vote.cxx"
    "${LIBRARY_DIR}/src/launcher.cxx"
    "${LIBRARY_DIR}/src/log_entry.cxx"
    "${LIBRARY_DIR}/src/srv_config.cxx"
    "${LIBRARY_DIR}/src/snapshot_sync_req.cxx"
    "${LIBRARY_DIR}/src/snapshot_sync_ctx.cxx"
    "${LIBRARY_DIR}/src/handle_timeout.cxx"
    "${LIBRARY_DIR}/src/handle_append_entries.cxx"
    "${LIBRARY_DIR}/src/cluster_config.cxx"
)


add_library(_nuraft ${SRCS})


if(NOT TARGET OpenSSL::Crypto)
    target_compile_definitions(_nuraft PRIVATE USE_BOOST_ASIO=1 BOOST_ASIO_STANDALONE=1 SSL_LIBRARY_NOT_FOUND=1)
else()
    target_compile_definitions(_nuraft PRIVATE USE_BOOST_ASIO=1 BOOST_ASIO_STANDALONE=1)
endif()

target_link_libraries (_nuraft PRIVATE clickhouse_common_io)
# We must have it PUBLIC here because some headers which depend on it directly
# included in clickhouse
target_compile_definitions(_nuraft PUBLIC USE_CLICKHOUSE_THREADS=1)
MESSAGE(STATUS "Will use clickhouse threads for NuRaft")

target_include_directories (_nuraft SYSTEM PRIVATE "${LIBRARY_DIR}/include/libnuraft")
# for some reason include "asio.h" directly without "boost/" prefix.
target_include_directories (_nuraft SYSTEM PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/boost/boost")

target_link_libraries (_nuraft PRIVATE boost::headers_only boost::coroutine)

if(TARGET OpenSSL::Crypto)
    target_link_libraries (_nuraft PRIVATE OpenSSL::Crypto OpenSSL::SSL)
endif()

target_include_directories (_nuraft SYSTEM PUBLIC "${LIBRARY_DIR}/include")

add_library(ch_contrib::nuraft ALIAS _nuraft)
