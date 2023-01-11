option(USE_LIBPROTOBUF_MUTATOR "Enable libprotobuf-mutator" ${ENABLE_FUZZING})

if (NOT USE_LIBPROTOBUF_MUTATOR)
    return()
endif()

set(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/libprotobuf-mutator)

add_library(_protobuf-mutator
    ${LIBRARY_DIR}/src/libfuzzer/libfuzzer_macro.cc
    ${LIBRARY_DIR}/src/libfuzzer/libfuzzer_mutator.cc
    ${LIBRARY_DIR}/src/binary_format.cc
    ${LIBRARY_DIR}/src/mutator.cc
    ${LIBRARY_DIR}/src/text_format.cc
    ${LIBRARY_DIR}/src/utf8_fix.cc)

target_include_directories(_protobuf-mutator BEFORE INTERFACE "${LIBRARY_DIR}")
target_include_directories(_protobuf-mutator BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/protobuf/src")

target_link_libraries(_protobuf-mutator ch_contrib::protobuf)

add_library(ch_contrib::protobuf_mutator ALIAS _protobuf-mutator)
