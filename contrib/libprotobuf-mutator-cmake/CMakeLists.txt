set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/libprotobuf-mutator)

add_library(protobuf-mutator
    ${LIBRARY_DIR}/src/libfuzzer/libfuzzer_macro.cc
    ${LIBRARY_DIR}/src/libfuzzer/libfuzzer_mutator.cc
    ${LIBRARY_DIR}/src/binary_format.cc
    ${LIBRARY_DIR}/src/mutator.cc
    ${LIBRARY_DIR}/src/text_format.cc
    ${LIBRARY_DIR}/src/utf8_fix.cc)

target_include_directories(protobuf-mutator BEFORE PRIVATE "${LIBRARY_DIR}")
target_include_directories(protobuf-mutator BEFORE PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/protobuf/src")

target_link_libraries(protobuf-mutator ${Protobuf_LIBRARY})
