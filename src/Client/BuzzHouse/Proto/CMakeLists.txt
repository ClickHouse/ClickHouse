protobuf_generate_cpp(clickhouse_buzzhouse_proto_sources clickhouse_buzzhouse_proto_headers SQLGrammar.proto)

add_library(clickhouse_buzzhouse_proto ${clickhouse_buzzhouse_proto_headers} ${clickhouse_buzzhouse_proto_sources})
target_include_directories(clickhouse_buzzhouse_proto SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(clickhouse_buzzhouse_proto PUBLIC ch_contrib::protoc)
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
target_compile_options(clickhouse_buzzhouse_proto PRIVATE "-w")
# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set_target_properties(clickhouse_buzzhouse_proto PROPERTIES CXX_CLANG_TIDY "")
