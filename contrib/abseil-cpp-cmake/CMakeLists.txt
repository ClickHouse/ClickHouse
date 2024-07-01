set(ABSL_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/abseil-cpp")
set(BUILD_TESTING OFF)
set(ABSL_PROPAGATE_CXX_STD ON)
add_subdirectory("${ABSL_ROOT_DIR}" "${ClickHouse_BINARY_DIR}/contrib/abseil-cpp")

add_library(_abseil_swiss_tables INTERFACE)

target_link_libraries(_abseil_swiss_tables INTERFACE
  absl::flat_hash_map
  absl::flat_hash_set
)

get_target_property(FLAT_HASH_MAP_INCLUDE_DIR absl::flat_hash_map INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories (_abseil_swiss_tables SYSTEM BEFORE INTERFACE ${FLAT_HASH_MAP_INCLUDE_DIR})

get_target_property(FLAT_HASH_SET_INCLUDE_DIR absl::flat_hash_set INTERFACE_INCLUDE_DIRECTORIES)
target_include_directories (_abseil_swiss_tables SYSTEM BEFORE INTERFACE ${FLAT_HASH_SET_INCLUDE_DIR})

add_library(ch_contrib::abseil_swiss_tables ALIAS _abseil_swiss_tables)

set(ABSL_FORMAT_SRC
    ${ABSL_ROOT_DIR}/absl/strings/internal/str_format/arg.cc
    ${ABSL_ROOT_DIR}/absl/strings/internal/str_format/bind.cc
    ${ABSL_ROOT_DIR}/absl/strings/internal/str_format/extension.cc
    ${ABSL_ROOT_DIR}/absl/strings/internal/str_format/float_conversion.cc
    ${ABSL_ROOT_DIR}/absl/strings/internal/str_format/output.cc
    ${ABSL_ROOT_DIR}/absl/strings/internal/str_format/parser.cc
)

add_library(_abseil_str_format ${ABSL_FORMAT_SRC})
target_include_directories(_abseil_str_format PUBLIC ${ABSL_ROOT_DIR})

add_library(ch_contrib::abseil_str_format ALIAS _abseil_str_format)
