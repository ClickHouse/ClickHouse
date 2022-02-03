option(USE_LIBPROTOBUF_MUTATOR "Enable libprotobuf-mutator" ${ENABLE_FUZZING})

if (NOT USE_LIBPROTOBUF_MUTATOR)
    return()
endif()

set(LibProtobufMutator_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libprotobuf-mutator")

if (NOT EXISTS "${LibProtobufMutator_SOURCE_DIR}/README.md")
    message (ERROR "submodule contrib/libprotobuf-mutator is missing. to fix try run: \n git submodule update --init")
endif()
