option (ENABLE_VORTEX "Enable the Vortex file format" ${ENABLE_LIBRARIES})

# Instrumented for MSan this library grows enough that linking `unit_tests_dbms` runs out of
# `R_X86_64_PC32` relocations - the binary no longer fits the 2 GiB the small code model allows.
if (SANITIZE STREQUAL "memory")
    set (ENABLE_VORTEX OFF)
endif()

# The same limit is hit by the fuzzers: `clickhouse_fuzzer` links the whole server instrumented for
# ASan and libFuzzer, and with this library it no longer fits the small code model either.
if (ENABLE_FUZZING)
    set (ENABLE_VORTEX OFF)
endif()

if (NOT ENABLE_VORTEX OR NOT TARGET ch_contrib::arrow)
    message(STATUS "Not using vortex")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_vortex)
clickhouse_config_crate_flags(_ch_rust_vortex)
target_include_directories(_ch_rust_vortex INTERFACE include)
add_library(ch_rust::vortex ALIAS _ch_rust_vortex)

# Strip internal symbols from the Vortex static library after it is built.
#
# Vortex archive ends up defining 268K global symbols - 19 MiB of names on their own.
# None of them are callable from outside: the only entry points are the C functions
# declared in `vortex_ffi.h`.

# Note: `clickhouse-stripped` drops the debug info but keeps `.symtab` and `.strtab`.
# Keeping only the entry point leaves `.text` byte for byte identical.
# `prql` and `polyglot` are stripped for the same reason.
if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    ch_find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        # Read the entry points out of the FFI header so that adding one cannot be
        # forgotten in this file. The header stays the only place that defines the
        # ABI surface.
        #
        # Every exported function is declared as `vortex_ffi_<name>(`. A comment that
        # mentions one never puts the parenthesis directly after the name, but comment
        # lines are skipped anyway, so that a future one cannot quietly add a symbol
        # to the keep list.
        set(_vortex_ffi_header "${CMAKE_CURRENT_SOURCE_DIR}/include/vortex_ffi.h")
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_vortex_ffi_header}")

        file(STRINGS "${_vortex_ffi_header}" _vortex_ffi_declarations REGEX "vortex_ffi_[a-z0-9_]+[ \t]*\\(")
        set(_vortex_ffi_symbols "")
        foreach (_declaration IN LISTS _vortex_ffi_declarations)
            if (_declaration MATCHES "^[ \t]*(//|\\*)")
                continue()
            endif()
            string(REGEX MATCHALL "vortex_ffi_[a-z0-9_]+[ \t]*\\(" _matches "${_declaration}")
            foreach (_match IN LISTS _matches)
                string(REGEX REPLACE "[ \t]*\\($" "" _symbol "${_match}")
                list(APPEND _vortex_ffi_symbols "${_symbol}")
            endforeach()
        endforeach()
        list(REMOVE_DUPLICATES _vortex_ffi_symbols)

        # Fail closed: stripping with an empty keep list would produce a
        # library with no entry points at all, and the failure would only
        # surface as undefined references at the very end of the build.
        if (NOT _vortex_ffi_symbols)
            message(FATAL_ERROR "No vortex_ffi_* entry points found in ${_vortex_ffi_header}. "
                "Either the header moved or its declaration style changed; "
                "stripping the library against an empty keep list would leave nothing to link against.")
        endif()

        list(LENGTH _vortex_ffi_symbols _vortex_ffi_symbol_count)
        message(STATUS "Vortex: keeping ${_vortex_ffi_symbol_count} FFI entry points when stripping")

        clickhouse_postprocess_rust_lib(_ch_rust_vortex
            STEP strip_rust_symbols
            SCRIPT "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
            ARGS "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}" ${_vortex_ffi_symbols}
            COMMENT "Stripping internal symbols from Vortex library"
        )
    endif ()
endif ()
