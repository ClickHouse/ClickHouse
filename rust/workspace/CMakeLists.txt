function(clickhouse_import_crate)
    # This is a workaround for Corrosion case sensitive build type matching in
    # _generator_add_cargo_targets(), that leads to different paths in
    # IMPORTED_LOCATION and real path of the library.
    #
    # It uses CMAKE_CONFIGURATION_TYPES and $<CONFIG>, so here we preserve the
    # case of ${CMAKE_BUILD_TYPE} in ${CMAKE_CONFIGURATION_TYPES}.
    if ("${CMAKE_BUILD_TYPE_UC}" STREQUAL "DEBUG")
        set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE};release")
    else()
        set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE};debug")
    endif()

    if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
        set(profile "")
    else()
        if (ENABLE_THINLTO)
            set(profile "release-thinlto")
        else()
            set(profile "release")
        endif()
    endif()

    corrosion_import_crate(${ARGN} NO_STD PROFILE ${profile} LOCKED FLAGS --offline)

endfunction()

# TODO: via properties
## -Wno-dollar-in-identifier-extension: cxx bridge complies names with '$'
## -Wno-unused-macros: unused CXXBRIDGE1_RUST_STRING
#set(CXXBRIDGE_CXXFLAGS "-Wno-dollar-in-identifier-extension -Wno-unused-macros")
#set(RUST_CXXFLAGS "${RUST_CXXFLAGS} ${CXXBRIDGE_CXXFLAGS}")
#message(STATUS "RUST_CXXFLAGS (for skim): ${RUST_CXXFLAGS}")

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)
corrosion_set_env_vars(_ch_rust_prql "CARGO_HOME=${CMAKE_CURRENT_BINARY_DIR}/prql_home")
corrosion_set_env_vars(_ch_rust_skim_rust "CARGO_HOME=${CMAKE_CURRENT_BINARY_DIR}/skim_home")

get_property(
    INTERFACE_PRQL_CARGO_FLAGS
    TARGET _ch_rust_prql
    PROPERTY INTERFACE_CORROSION_CARGO_FLAGS
)
SET(INTERFACE_PRQL_CARGO_FLAGS "${INTERFACE_PRQL_CARGO_FLAGS};--config=${RUST_CONFIG_FILE}")
set_property(
        TARGET _ch_rust_prql
        PROPERTY INTERFACE_CORROSION_CARGO_FLAGS "${INTERFACE_PRQL_CARGO_FLAGS}"
)
set_property(
        TARGET _ch_rust_skim_rust
        PROPERTY INTERFACE_CORROSION_CARGO_FLAGS "${INTERFACE_PRQL_CARGO_FLAGS}"
)

get_property(
        INTERFACE_PRQL_CARGO_FLAGS
        TARGET _ch_rust_prql
        PROPERTY INTERFACE_CORROSION_CARGO_FLAGS
)

message(STATUS "Workspace libs cargo flags: ${INTERFACE_PRQL_CARGO_FLAGS}")

target_include_directories(_ch_rust_prql INTERFACE prql/include)
add_library(ch_rust::prql ALIAS _ch_rust_prql)

target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)