set(AEROSPIKE_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/aerospike/src)

set(SRCS
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/_bin.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_batch.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_index.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_info.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_key.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_query.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_scan.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_stats.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/aerospike_udf.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_address.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_admin.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_async.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_batch.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_cluster.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_command.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_config.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_error.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_event.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_event_ev.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_event_event.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_event_none.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_event_uv.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_host.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_info.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_job.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_key.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_lookup.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_node.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_operations.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_partition.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_peers.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_pipe.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_policy.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_predexp.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_proto.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_query.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_query_validate.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_record.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_record_hooks.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_record_iterator.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_scan.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_shm_cluster.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_socket.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_tls.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/as_udf.c
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/version.c
)

set(HEADERS
    ${AEROSPIKE_SOURCE_DIR}/main/aerospike/_bin.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_batch.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_index.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_info.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_key.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_query.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_scan.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_stats.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/aerospike_udf.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_address.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_admin.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_async.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_async_proto.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_batch.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_bin.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_cluster.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_command.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_config.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_cpu.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_error.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_event.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_event_internal.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_host.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_info.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_job.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_key.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_list_operations.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_listener.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_lookup.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_map_operations.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_node.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_operations.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_partition.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_peers.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_pipe.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_policy.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_poll.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_predexp.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_proto.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_query.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_query_validate.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_record.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_record_iterator.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_scan.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_shm_cluster.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_socket.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_status.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_tls.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/as_udf.h
    ${AEROSPIKE_SOURCE_DIR}/include/aerospike/version.h
)

#configure_file ( ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/luajit/src/luaconf.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/luaconf.h)
#add_headers_and_sources(aerospike ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/luajit/src)

add_headers_and_sources(aerospike ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/common/src/main/aerospike)
add_headers_and_sources(aerospike ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/common/src/main/citrusleaf)
add_headers_and_sources(aerospike ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/mod-lua/src/main)

# add_library(aerospike ${LINK_MODE} ${SRCS} ${HEADERS})
add_library(aerospike ${SRCS} ${HEADERS} ${aerospike_sources})

target_include_directories(aerospike PRIVATE ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/luajit/src ${ClickHouse_BINARY_DIR}/contrib/aerospike/modules/luajit) # ${CMAKE_CURRENT_BINARY_DIR}/include/)
# find_package(OpenSSL QUIET)
# if(OPENSSL_FOUND)
#     message(STATUS "Aerospike using OpenSSL from ${OPENSSL_ROOT_DIR}")
#     message("Library: ${OPENSSL_LIBRARIES}")
# else()
#     message("Aerospike OpenSSL not found")
# endif()

target_compile_definitions(aerospike PRIVATE _GNU_SOURCE)
target_compile_definitions(aerospike PRIVATE USE_LUAJIT=1)
target_compile_definitions(aerospike PRIVATE USE_XDR=1)

message(STATUS "Aerospike using OpenSSL from ${OPENSSL_LIBRARIES}")
target_link_libraries(aerospike PRIVATE ${OPENSSL_LIBRARIES})
target_link_libraries(aerospike PRIVATE ${LINE_EDITING_LIBS})
message (STATUS "line editing ${LINE_EDITING_LIBS}")

target_link_libraries(aerospike PRIVATE liblua)

message(STATUS "Aerospike include dir OpenSSL from ${OPENSSL_INCLUDE_DIR}")
target_include_directories(aerospike PRIVATE ${OPENSSL_INCLUDE_DIR})

# if (READLINE_INCLUDE_DIR)
#    message (STATUS "readline ${READLINE_INCLUDE_DIR}")
#    target_include_directories (aerospike PRIVATE ${READLINE_INCLUDE_DIR})
# endif ()

target_include_directories(aerospike
        SYSTEM PUBLIC
        ${AEROSPIKE_SOURCE_DIR}/include
        ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/common/src/include
        ${ClickHouse_SOURCE_DIR}/contrib/aerospike/modules/mod-lua/src/include
#        ${OPENSSL_ROOT_DIR}/include/openssl/include
)
#($<BUILD_INTERFACE:${AEROSPIKE_SOURCE_DIR}/include>
#    $<INSTALL_INTERFACE:src/include>
#    PRIVATE ${AEROSPIKE_SOURCE_DIR}/main

