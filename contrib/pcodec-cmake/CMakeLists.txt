option (ENABLE_PCO "Enable pco (pcodec) compression codec" ${ENABLE_LIBRARIES})
if (NOT ENABLE_PCO)
    message(STATUS "Not using pco")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_pco)
clickhouse_config_crate_flags(_ch_rust_pco)
target_include_directories(_ch_rust_pco INTERFACE include)
add_library(ch_rust::pco ALIAS _ch_rust_pco)

# Strip internal symbols from the pco static library; only the two C FFI
# entry points need to stay visible (the pco crate and its dependencies
# export a large number of internal symbols). Same guards as prql/polyglot.
if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        set(_pco_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_pco.a")
        add_custom_target(strip_pco_symbols
            COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                "${_pco_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                pco_compress pco_decompress
            DEPENDS cargo-build__ch_rust_pco
            COMMENT "Stripping internal symbols from pco library"
            VERBATIM
        )
        add_dependencies(_ch_rust_pco strip_pco_symbols)
        # Ensure localize runs before strip (they both modify the same .a)
        if (TARGET localize_rust_c__ch_rust_pco)
            add_dependencies(strip_pco_symbols localize_rust_c__ch_rust_pco)
        endif()
    endif ()
endif ()
