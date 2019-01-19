set(AEROSPIKE_DIR ${ClickHouse_SOURCE_DIR}/contrib/aerospike)

set(SRCS
    ${AEROSPIKE_DIR}/src/main/aerospike/_bin.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_batch.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_index.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_info.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_key.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_query.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_scan.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_stats.c
    ${AEROSPIKE_DIR}/src/main/aerospike/aerospike_udf.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_address.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_admin.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_async.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_batch.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_cluster.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_command.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_config.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_error.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_event.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_event_ev.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_event_event.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_event_none.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_event_uv.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_host.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_info.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_job.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_key.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_lookup.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_node.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_operations.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_partition.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_peers.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_pipe.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_policy.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_predexp.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_proto.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_query.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_query_validate.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_record.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_record_hooks.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_record_iterator.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_scan.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_shm_cluster.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_socket.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_tls.c
    ${AEROSPIKE_DIR}/src/main/aerospike/as_udf.c
    ${AEROSPIKE_DIR}/src/main/aerospike/version.c
)

add_library(aerospike ${LINK_MODE}  ${SRCS})

target_include_directories(aerospike SYSTEM PUBLIC ${AEROSPIKE_DIR}/src/include)
