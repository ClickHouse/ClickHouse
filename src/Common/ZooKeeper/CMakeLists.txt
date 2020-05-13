include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/etcd_protos)

get_filename_component(rpc_proto "${CMAKE_CURRENT_SOURCE_DIR}/etcd_protos/rpc.proto" ABSOLUTE)
PROTOBUF_GENERATE_GRPC_CPP(EtcdProtosGRPC_Srcs EtcdProtosGRPC_Hdrs ${rpc_proto})
file(GLOB PROTO_DEF "${CMAKE_CURRENT_SOURCE_DIR}/etcd_protos/*.proto")
protobuf_generate_cpp(EtcdProtos_Srcs EtcdProtos_Hdrs ${PROTO_DEF})

add_headers_and_sources(clickhouse_common_zookeeper .)

add_library(clickhouse_common_zookeeper ${clickhouse_common_zookeeper_headers} ${clickhouse_common_zookeeper_sources} ${EtcdProtosGRPC_Srcs} ${EtcdProtosGRPC_Hdrs} ${EtcdProtos_Srcs} ${EtcdProtos_Hdrs})

target_link_libraries (clickhouse_common_zookeeper PUBLIC clickhouse_common_io common PRIVATE string_utils PUBLIC grpc++)
target_include_directories(clickhouse_common_zookeeper PUBLIC ${DBMS_INCLUDE_DIR})

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
