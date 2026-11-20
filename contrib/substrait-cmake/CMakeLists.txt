option(ENABLE_SUBSTRAIT "Enable Substrait" ${ENABLE_LIBRARIES})

if(NOT ENABLE_SUBSTRAIT)
    message(STATUS "Not using Substrait")
    return()
endif()

if(NOT TARGET ch_contrib::protoc)
    message(STATUS "Not using Substrait because protobuf is not available")
    return()
endif()

set(Substrait_PROTO_DIR "${ClickHouse_SOURCE_DIR}/contrib/substrait/proto")

set(_substrait_changelog "${ClickHouse_SOURCE_DIR}/contrib/substrait/CHANGELOG.md")
file(STRINGS "${_substrait_changelog}" _substrait_version_line REGEX "^## \\[[0-9]+\\.[0-9]+\\.[0-9]+\\]" LIMIT_COUNT 1)
string(REGEX MATCH "([0-9]+)\\.([0-9]+)\\.([0-9]+)" SUBSTRAIT_VERSION "${_substrait_version_line}")
if(NOT SUBSTRAIT_VERSION)
    message(FATAL_ERROR "Cannot read the Substrait version from ${_substrait_changelog}")
endif()
set(SUBSTRAIT_VERSION_MAJOR ${CMAKE_MATCH_1})
set(SUBSTRAIT_VERSION_MINOR ${CMAKE_MATCH_2})
set(SUBSTRAIT_VERSION_PATCH ${CMAKE_MATCH_3})
message(STATUS "Using Substrait ${SUBSTRAIT_VERSION}")

set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_substrait_changelog}")
configure_file(version_vendored.h.in "${CMAKE_CURRENT_BINARY_DIR}/substrait/version_vendored.h")

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
