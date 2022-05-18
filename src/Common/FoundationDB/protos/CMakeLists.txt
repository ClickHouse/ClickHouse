add_glob(proto_files "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
protobuf_generate_cpp(proto_srcs proto_hdrs ${proto_files})

add_library(clickhouse_common_foundationdb_protos ${proto_hdrs} ${proto_srcs})
target_include_directories(clickhouse_common_foundationdb_protos PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(clickhouse_common_foundationdb_protos PUBLIC ch_contrib::protobuf)
target_compile_options(clickhouse_common_foundationdb_protos PUBLIC -Wno-reserved-identifier)