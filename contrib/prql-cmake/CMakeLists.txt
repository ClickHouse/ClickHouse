option (ENABLE_PRQL "Enable prql" ${ENABLE_LIBRARIES})
if (NOT ENABLE_PRQL)
    message(STATUS "Not using prql")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_prql)
clickhouse_config_crate_flags(_ch_rust_prql)
target_include_directories(_ch_rust_prql INTERFACE include)
add_library(ch_rust::prql ALIAS _ch_rust_prql)

# Strip internal symbols from the PRQL static library after it is built.
# Only the C FFI functions need to remain visible; all other symbols (67K+)
# from the chumsky parser combinator are internal and produce enormous names.
# Skip in tidy builds where CMAKE_AR is a dummy script that doesn't extract archives.
# Skip in sanitizer builds where instrumentation adds special symbols (e.g.,
# ___asan_globals_registered) that the partial link + strip process breaks.
# Skip on Darwin where objects are Mach-O (ld.lld -r only handles ELF).
# Skip on ppc64le where ld.lld -r produces broken .eh_frame relocations.
if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    # Use ld.lld for partial linking — it's a cross-linker that handles all ELF architectures,
    # unlike the host ld which may not support the target architecture.
    find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        set(_prql_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_prql.a")
        add_custom_target(strip_prql_symbols
            COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                "${_prql_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                prql_to_sql prql_free_pointer
            DEPENDS cargo-build__ch_rust_prql
            COMMENT "Stripping internal symbols from PRQL library"
            VERBATIM
        )
        add_dependencies(_ch_rust_prql strip_prql_symbols)
        # Ensure localize runs before strip (they both modify the same .a)
        if (TARGET localize_rust_c__ch_rust_prql)
            add_dependencies(strip_prql_symbols localize_rust_c__ch_rust_prql)
        endif()
    endif ()
endif ()
