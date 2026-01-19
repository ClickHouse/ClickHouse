# Generate protobuf files directly in the build directory (not in nested src/... path)
# This matches ClickHouse's pattern for other protobuf files (e.g., clickhouse_grpc.grpc.pb.h, SQLGrammar.pb.h)
protobuf_generate_cpp(clickhouse_manifest_proto_sources clickhouse_manifest_proto_headers manifest.proto)

add_library(clickhouse_manifest_proto ${clickhouse_manifest_proto_headers} ${clickhouse_manifest_proto_sources})
target_include_directories(clickhouse_manifest_proto SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(clickhouse_manifest_proto PUBLIC ch_contrib::protoc)
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
target_compile_options(clickhouse_manifest_proto PRIVATE "-w")
# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set_target_properties(clickhouse_manifest_proto PROPERTIES CXX_CLANG_TIDY "")

