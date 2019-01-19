set(AEROSPIKE_DIR ${ClickHouse_SOURCE_DIR}/contrib/aerospike)

set(
    ${AEROSPIKE_DIR}/src_bin.c
    ${AEROSPIKE_DIR}/aerospike.c
    ${AEROSPIKE_DIR}/src/aerospike_batch.c
    ${AEROSPIKE_DIR}/src/aerospike_index.c
    ${AEROSPIKE_DIR}/src/aerospike_info.c
    ${AEROSPIKE_DIR}/src/aerospike_key.c
    ${AEROSPIKE_DIR}/src/aerospike_query.c
    ${AEROSPIKE_DIR}/src/aerospike_scan.c
    ${AEROSPIKE_DIR}/src/aerospike_stats.c
    ${AEROSPIKE_DIR}/src/aerospike_udf.c
    ${AEROSPIKE_DIR}/src/as_address.c
    ${AEROSPIKE_DIR}/src/as_admin.c
    ${AEROSPIKE_DIR}/src/as_async.c
    ${AEROSPIKE_DIR}/src/as_batch.c
    ${AEROSPIKE_DIR}/src/as_cluster.c
    ${AEROSPIKE_DIR}/src/as_command.c
    ${AEROSPIKE_DIR}/src/as_config.c
    ${AEROSPIKE_DIR}/src/as_error.c
    ${AEROSPIKE_DIR}/src/as_event.c
    ${AEROSPIKE_DIR}/src/as_event_ev.c
    ${AEROSPIKE_DIR}/src/as_event_event.c
    ${AEROSPIKE_DIR}/src/as_event_none.c
    ${AEROSPIKE_DIR}/src/as_event_uv.c
    ${AEROSPIKE_DIR}/src/as_host.c
    ${AEROSPIKE_DIR}/src/as_info.c
    ${AEROSPIKE_DIR}/src/as_job.c
    ${AEROSPIKE_DIR}/src/as_key.c
    ${AEROSPIKE_DIR}/src/as_lookup.c
    ${AEROSPIKE_DIR}/src/as_node.c
    ${AEROSPIKE_DIR}/src/as_operations.c
    ${AEROSPIKE_DIR}/src/as_partition.c
    ${AEROSPIKE_DIR}/src/as_peers.c
    ${AEROSPIKE_DIR}/src/as_pipe.c
    ${AEROSPIKE_DIR}/src/as_policy.c
    ${AEROSPIKE_DIR}/src/as_predexp.c
    ${AEROSPIKE_DIR}/src/as_proto.c
    ${AEROSPIKE_DIR}/src/as_query.c
    ${AEROSPIKE_DIR}/src/as_query_validate.c
    ${AEROSPIKE_DIR}/src/as_record.c
    ${AEROSPIKE_DIR}/src/as_record_hooks.c
    ${AEROSPIKE_DIR}/src/as_record_iterator.c
    ${AEROSPIKE_DIR}/src/as_scan.c
    ${AEROSPIKE_DIR}/src/as_shm_cluster.c
    ${AEROSPIKE_DIR}/src/as_socket.c
    ${AEROSPIKE_DIR}/src/as_tls.c
    ${AEROSPIKE_DIR}/src/as_udf.c
    ${AEROSPIKE_DIR}/src/version.c
)

add_library(aerospike PRIVATE ${SRCS})

target_include_directories(cppkafka SYSTEM PUBLIC ${AEROSPIKE_DIR}/src/include)
