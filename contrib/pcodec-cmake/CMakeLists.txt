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
    ch_find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        clickhouse_postprocess_rust_lib(_ch_rust_pco
            STEP strip_rust_symbols
            SCRIPT "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
            ARGS "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}" pco_compress pco_decompress
            COMMENT "Stripping internal symbols from pco library"
        )
    endif ()
endif ()
