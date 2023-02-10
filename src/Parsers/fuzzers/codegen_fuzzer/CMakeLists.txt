set (CURRENT_DIR_IN_SOURCES "${ClickHouse_SOURCE_DIR}/src/Parsers/fuzzers/codegen_fuzzer")
set (CURRENT_DIR_IN_BINARY "${ClickHouse_BINARY_DIR}/src/Parsers/fuzzers/codegen_fuzzer")

# Copy scripts and template file to build directory to generate .proto and .cpp file from them
configure_file(
    "${CURRENT_DIR_IN_SOURCES}/gen.py"
    "${CURRENT_DIR_IN_BINARY}/gen.py"
    COPYONLY)
configure_file(
    "${CURRENT_DIR_IN_SOURCES}/update.sh"
    "${CURRENT_DIR_IN_BINARY}/update.sh"
    COPYONLY)

configure_file(
    "${CURRENT_DIR_IN_SOURCES}/clickhouse-template.g"
    "${CURRENT_DIR_IN_BINARY}/clickhouse-template.g"
    COPYONLY)

# Note that it depends on all.dict file!
add_custom_command(
    OUTPUT
        "${CURRENT_DIR_IN_BINARY}/clickhouse.g"
    COMMAND ./update.sh "${ClickHouse_SOURCE_DIR}/tests/fuzz/all.dict"
)

add_custom_command(
    OUTPUT
        "${CURRENT_DIR_IN_BINARY}/out.cpp"
        "${CURRENT_DIR_IN_BINARY}/out.proto"
    COMMAND python3 gen.py clickhouse.g out.cpp out.proto
    DEPENDS "${CURRENT_DIR_IN_BINARY}/clickhouse.g"
)

set(PROTOBUF_GENERATE_CPP_APPEND_PATH TRUE)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS "${CURRENT_DIR_IN_BINARY}/out.proto")
set(FUZZER_SRCS codegen_select_fuzzer.cpp "${CURRENT_DIR_IN_BINARY}/out.cpp" ${PROTO_SRCS} ${PROTO_HDRS})

set(CMAKE_INCLUDE_CURRENT_DIR TRUE)

clickhouse_add_executable(codegen_select_fuzzer ${FUZZER_SRCS})

set_source_files_properties("${PROTO_SRCS}" "out.cpp" PROPERTIES COMPILE_FLAGS "-Wno-reserved-identifier")

target_include_directories(codegen_select_fuzzer SYSTEM BEFORE PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(codegen_select_fuzzer PRIVATE ch_contrib::protobuf_mutator ch_contrib::protoc dbms ${LIB_FUZZING_ENGINE})
