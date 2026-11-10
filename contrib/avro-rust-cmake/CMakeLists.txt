# `ENABLE_AVRO_RUST` is a dependent option declared in `contrib/CMakeLists.txt` once `ENABLE_AVRO` and `ENABLE_RUST` are known.
if (NOT ENABLE_AVRO_RUST)
    message(STATUS "Not using avro_rust")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_avro)
clickhouse_config_crate_flags(_ch_rust_avro)
target_include_directories(_ch_rust_avro INTERFACE include)
add_library(ch_rust::avro ALIAS _ch_rust_avro)

# Strip internal symbols from the avro static library; only the `avro_*` C ABI
# declared in `avro_rust.h` stays visible. Same guards as prql/polyglot/pco/vortex.
if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        # The header is the only place that defines the ABI surface: every exported
        # function is declared as `avro_<name>(`. Comment lines are skipped.
        set(_avro_rust_header "${CMAKE_CURRENT_SOURCE_DIR}/include/avro_rust.h")
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_avro_rust_header}")

        file(STRINGS "${_avro_rust_header}" _avro_rust_declarations REGEX "avro_[a-z0-9_]+[ \t]*\\(")
        set(_avro_rust_symbols "")
        foreach (_declaration IN LISTS _avro_rust_declarations)
            if (_declaration MATCHES "^[ \t]*(//|\\*)")
                continue()
            endif()
            string(REGEX MATCHALL "avro_[a-z0-9_]+[ \t]*\\(" _matches "${_declaration}")
            foreach (_match IN LISTS _matches)
                string(REGEX REPLACE "[ \t]*\\($" "" _symbol "${_match}")
                list(APPEND _avro_rust_symbols "${_symbol}")
            endforeach()
        endforeach()
        list(REMOVE_DUPLICATES _avro_rust_symbols)

        # Fail closed: an empty keep list would leave a library with no entry points.
        if (NOT _avro_rust_symbols)
            message(FATAL_ERROR "No avro_* entry points found in ${_avro_rust_header}. "
                "Either the header moved or its declaration style changed; "
                "stripping the library against an empty keep list would leave nothing to link against.")
        endif()

        list(LENGTH _avro_rust_symbols _avro_rust_symbol_count)
        message(STATUS "Avro Rust: keeping ${_avro_rust_symbol_count} FFI entry points when stripping")

        set(_avro_rust_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_avro.a")
        add_custom_target(strip_avro_symbols
            COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                "${_avro_rust_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                ${_avro_rust_symbols}
            DEPENDS cargo-build__ch_rust_avro
            COMMENT "Stripping internal symbols from avro library"
            VERBATIM
        )
        add_dependencies(_ch_rust_avro strip_avro_symbols)
        # Ensure localize runs before strip (they both modify the same .a)
        if (TARGET localize_rust_c__ch_rust_avro)
            add_dependencies(strip_avro_symbols localize_rust_c__ch_rust_avro)
        endif()
    endif ()
endif ()
