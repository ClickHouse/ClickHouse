if (OS_FREEBSD)
    # Right nix/libc requires fspacectl and it had been added only since FreeBSD14.
    # And since sysroot has older libraries you will got undefined reference for clickhouse binary.
    #
    # But likely everything should work without this syscall, however it is not
    # possible right now to gently override libraries versions for dependencies,
    # and forking rust modules is a little bit too much for this thing.
    #
    # You can take a look at the details in the following issue [1].
    #
    #   [1]: https://github.com/rust-lang/cargo/issues/5640
    #
    # Update 2024-04: Now prql also requires getrandom() via std::sys::pal::unix::rand::imp::getrandom_fill_bytes
    message(STATUS "Rust build is disabled for FreeBSD because we use old sysroot files")
    return()
endif()

option (ENABLE_PRQL "Enable prql" ${ENABLE_LIBRARIES})
option (ENABLE_SKIM "Enable skim" ${ENABLE_LIBRARIES})
option (ENABLE_POLYGLOT "Enable polyglot SQL transpiler" ${ENABLE_LIBRARIES})
option (ENABLE_VORTEX "Enable the Vortex file format" ${ENABLE_LIBRARIES})

# Instrumented for MSan this library grows enough that linking `unit_tests_dbms` runs out of
# `R_X86_64_PC32` relocations - the binary no longer fits the 2 GiB the small code model allows.
if (SANITIZE STREQUAL "memory")
    set (ENABLE_VORTEX OFF)
endif()

if (NOT ENABLE_PRQL AND NOT ENABLE_SKIM AND NOT ENABLE_POLYGLOT AND NOT ENABLE_VORTEX)
    message(STATUS "prql, skim, polyglot, and vortex are disabled")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

if (ENABLE_PRQL)
    clickhouse_config_crate_flags(_ch_rust_prql)
    target_include_directories(_ch_rust_prql INTERFACE prql/include)
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
endif()

if (ENABLE_POLYGLOT)
    clickhouse_config_crate_flags(_ch_rust_polyglot)
    target_include_directories(_ch_rust_polyglot INTERFACE polyglot/include)
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
endif()

if (ENABLE_SKIM)
    clickhouse_config_crate_flags(_ch_rust_skim_rust)
    target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
    add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
endif()

if (ENABLE_VORTEX AND TARGET ch_contrib::arrow)
    clickhouse_config_crate_flags(_ch_rust_vortex)
    target_include_directories(_ch_rust_vortex INTERFACE vortex/include)
    add_library(ch_rust::vortex ALIAS _ch_rust_vortex)

    # Strip internal symbols from the Vortex static library after it is built.
    #
    # Vortex archive ends up defining 268K global symbols - 19 MiB of names on their own.
    # None of them are callable from outside: the only entry points are the C functions
    # declared in `vortex_ffi.h`.

    # Note: `clickhouse-stripped` drops the debug info but keeps `.symtab` and `.strtab`.
    # Keeping only the entry point leaves `.text` byte for byte identical.
    # `prql` and `polyglot` above are stripped for the same reason.
    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            # Read the entry points out of the FFI header so that adding one cannot be
            # forgotten in this file. The header stays the only place that defines the
            # ABI surface.
            #
            # Every exported function is declared as `vortex_ffi_<name>(`. A comment that
            # mentions one never puts the parenthesis directly after the name, but comment
            # lines are skipped anyway, so that a future one cannot quietly add a symbol
            # to the keep list.
            set(_vortex_ffi_header "${CMAKE_CURRENT_SOURCE_DIR}/vortex/include/vortex_ffi.h")
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

            set(_vortex_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_vortex.a")
            add_custom_target(strip_vortex_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_vortex_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    ${_vortex_ffi_symbols}
                DEPENDS cargo-build__ch_rust_vortex
                COMMENT "Stripping internal symbols from Vortex library"
                VERBATIM
            )
            add_dependencies(_ch_rust_vortex strip_vortex_symbols)
            # Ensure localize runs before strip (they both modify the same .a)
            if (TARGET localize_rust_c__ch_rust_vortex)
                add_dependencies(strip_vortex_symbols localize_rust_c__ch_rust_vortex)
            endif()
        endif ()
    endif ()
endif()

add_subdirectory (wasmtime)
