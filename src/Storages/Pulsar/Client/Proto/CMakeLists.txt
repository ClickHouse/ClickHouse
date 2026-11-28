protobuf_generate_cpp(clickhouse_pulsar_proto_sources clickhouse_pulsar_proto_headers PulsarApi.proto MetadataViews.proto)

add_library(clickhouse_pulsar_proto ${clickhouse_pulsar_proto_headers} ${clickhouse_pulsar_proto_sources})
target_include_directories(clickhouse_pulsar_proto SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(clickhouse_pulsar_proto PUBLIC ch_contrib::protobuf)
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
target_compile_options(clickhouse_pulsar_proto PRIVATE "-w")
# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set_target_properties(clickhouse_pulsar_proto PROPERTIES CXX_CLANG_TIDY "")
