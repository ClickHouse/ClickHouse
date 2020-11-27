PROTOBUF_GENERATE_GRPC_CPP(clickhouse_etcd_proto_sources clickhouse_etcd_proto_headers rpc.proto kv.proto)

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(clickhouse_etcd_protos ${clickhouse_etcd_proto_sources} ${clickhouse_etcd_proto_headers})
target_include_directories(clickhouse_etcd_protos SYSTEM PUBLIC ${gRPC_INCLUDE_DIRS} ${Protobuf_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (clickhouse_etcd_protos PUBLIC ${gRPC_LIBRARIES})
