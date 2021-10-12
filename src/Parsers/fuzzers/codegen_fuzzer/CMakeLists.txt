find_package(Protobuf REQUIRED)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS "out.proto")
set(FUZZER_SRCS codegen_select_fuzzer.cpp out.cpp ${PROTO_SRCS} ${PROTO_HDRS})

set(CMAKE_INCLUDE_CURRENT_DIR TRUE)

add_executable(codegen_select_fuzzer ${FUZZER_SRCS})

target_include_directories(codegen_select_fuzzer BEFORE PRIVATE "${Protobuf_INCLUDE_DIRS}")
target_include_directories(codegen_select_fuzzer BEFORE PRIVATE "${LibProtobufMutator_SOURCE_DIR}")
target_include_directories(codegen_select_fuzzer BEFORE PRIVATE "${LibProtobufMutator_SOURCE_DIR}/src")
target_link_libraries(codegen_select_fuzzer PRIVATE clickhouse_parsers protobuf-mutator ${Protobuf_LIBRARIES} ${LIB_FUZZING_ENGINE})