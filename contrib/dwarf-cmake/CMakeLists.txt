option (ENABLE_DWARF_PARSER "Enable DWARF input format" ${ENABLE_LIBRARIES})

if (SANITIZE STREQUAL "memory" OR OS_DARWIN OR OS_FREEBSD OR OS_WASM)
    set (ENABLE_DWARF_PARSER OFF)
endif()

if (NOT ENABLE_DWARF_PARSER)
    message(STATUS "Not using dwarf")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_dwarf)
clickhouse_config_crate_flags(_ch_rust_dwarf)
target_include_directories(_ch_rust_dwarf INTERFACE include)
add_library(ch_rust::dwarf ALIAS _ch_rust_dwarf)

if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        set(_dwarf_rust_header "${CMAKE_CURRENT_SOURCE_DIR}/include/dwarf_rust.h")
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_dwarf_rust_header}")

        file(STRINGS "${_dwarf_rust_header}" _dwarf_rust_declarations REGEX "ch_dwarf_[a-z0-9_]+[ \t]*\\(")
        set(_dwarf_rust_symbols "")
        foreach (_declaration IN LISTS _dwarf_rust_declarations)
            if (_declaration MATCHES "^[ \t]*(//|\\*)")
                continue()
            endif()
            string(REGEX MATCHALL "ch_dwarf_[a-z0-9_]+[ \t]*\\(" _matches "${_declaration}")
            foreach (_match IN LISTS _matches)
                string(REGEX REPLACE "[ \t]*\\($" "" _symbol "${_match}")
                list(APPEND _dwarf_rust_symbols "${_symbol}")
            endforeach()
        endforeach()
        list(REMOVE_DUPLICATES _dwarf_rust_symbols)

        if (NOT _dwarf_rust_symbols)
            message(FATAL_ERROR "No ch_dwarf_* entry points found in ${_dwarf_rust_header}.")
        endif()

        set(_dwarf_rust_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_dwarf.a")
        add_custom_target(strip_dwarf_symbols
            COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                "${_dwarf_rust_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                ${_dwarf_rust_symbols}
            DEPENDS cargo-build__ch_rust_dwarf
            COMMENT "Stripping internal symbols from DWARF Rust library"
            VERBATIM
        )
        add_dependencies(_ch_rust_dwarf strip_dwarf_symbols)
        if (TARGET localize_rust_c__ch_rust_dwarf)
            add_dependencies(strip_dwarf_symbols localize_rust_c__ch_rust_dwarf)
        endif()
    endif ()
endif ()
