option (ENABLE_POLYGLOT "Enable polyglot SQL transpiler" ${ENABLE_LIBRARIES})
if (NOT ENABLE_POLYGLOT)
    message(STATUS "Not using polyglot")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_polyglot)
clickhouse_config_crate_flags(_ch_rust_polyglot)
target_include_directories(_ch_rust_polyglot INTERFACE include)
add_library(ch_rust::polyglot ALIAS _ch_rust_polyglot)

if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    ch_find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        clickhouse_postprocess_rust_lib(_ch_rust_polyglot
            STEP strip_rust_symbols
            SCRIPT "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
            ARGS "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}" polyglot_transpile polyglot_free_pointer
            COMMENT "Stripping internal symbols from polyglot library"
        )
    endif ()
endif ()
