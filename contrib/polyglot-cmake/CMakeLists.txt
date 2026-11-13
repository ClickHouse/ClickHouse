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
    find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        set(_polyglot_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_polyglot.a")
        add_custom_target(strip_polyglot_symbols
            COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                "${_polyglot_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                polyglot_transpile polyglot_free_pointer
            DEPENDS cargo-build__ch_rust_polyglot
            COMMENT "Stripping internal symbols from polyglot library"
            VERBATIM
        )
        add_dependencies(_ch_rust_polyglot strip_polyglot_symbols)
        # Ensure localize runs before strip (they both modify the same .a)
        if (TARGET localize_rust_c__ch_rust_polyglot)
            add_dependencies(strip_polyglot_symbols localize_rust_c__ch_rust_polyglot)
        endif()
    endif ()
endif ()
