# Disabled by default as nothing uses it yet.
option(ENABLE_SUBSTRAIT "Enable Substrait" OFF)

if(NOT ENABLE_SUBSTRAIT)
    message(STATUS "Not using Substrait")
    return()
endif()

if(NOT TARGET ch_contrib::protoc)
    message(STATUS "Not using Substrait because protobuf is not available")
    return()
endif()

set(Substrait_PROTO_DIR "${ClickHouse_SOURCE_DIR}/contrib/substrait/proto")

protobuf_generate(LANGUAGE cpp OUT_VAR substrait_proto_files
    IMPORT_DIRS "${Substrait_PROTO_DIR}" "${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src"
    PROTOS
        "${Substrait_PROTO_DIR}/substrait/algebra.proto"
        "${Substrait_PROTO_DIR}/substrait/extensions/extensions.proto"
        "${Substrait_PROTO_DIR}/substrait/plan.proto"
        "${Substrait_PROTO_DIR}/substrait/type.proto"
)

add_library(_substrait ${substrait_proto_files})
target_include_directories(_substrait SYSTEM PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(_substrait PUBLIC ch_contrib::protobuf)

add_library(ch_contrib::substrait ALIAS _substrait)
