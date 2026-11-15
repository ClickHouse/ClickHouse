PROTOBUF_GENERATE_GRPC_CPP(clickhouse_grpc_proto_sources clickhouse_grpc_proto_headers clickhouse_grpc.proto)

add_library(clickhouse_grpc_protos ${clickhouse_grpc_proto_headers} ${clickhouse_grpc_proto_sources})
target_include_directories(clickhouse_grpc_protos SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (clickhouse_grpc_protos PUBLIC ch_contrib::grpc)
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
target_compile_options(clickhouse_grpc_protos PRIVATE "-w")
# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set_target_properties(clickhouse_grpc_protos PROPERTIES CXX_CLANG_TIDY "")
