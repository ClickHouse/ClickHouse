# The Pulsar binary protocol is protobuf-based. Generate the C++ for the protocol definition into a
# small library of its own, so the generated code (which must not be linted or compiled with our
# warnings) is isolated from the hand-written client, which is built as part of `dbms`.
protobuf_generate_cpp(pulsar_proto_sources pulsar_proto_headers PulsarApi.proto)

add_library(clickhouse_pulsar_proto ${pulsar_proto_headers} ${pulsar_proto_sources})
target_include_directories(clickhouse_pulsar_proto SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(clickhouse_pulsar_proto PUBLIC ch_contrib::protobuf)
# Ignore warnings while compiling the protobuf-generated *.pb.h and *.pb.cc files.
target_compile_options(clickhouse_pulsar_proto PRIVATE "-w")
# Disable clang-tidy for the protobuf-generated *.pb.h and *.pb.cc files.
set_target_properties(clickhouse_pulsar_proto PROPERTIES CXX_CLANG_TIDY "")
