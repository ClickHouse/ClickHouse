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

if (NOT ENABLE_PRQL AND NOT ENABLE_SKIM AND NOT ENABLE_POLYGLOT)
    message(STATUS "prql, skim, and polyglot are disabled")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

# Strip internal symbols from a Rust static library after it is built, keeping only
# the given C FFI entry points visible.  For prql, all other symbols (67K+) from the
# chumsky parser combinator are internal and produce enormous names.
#
# Reads the archive consumers of ${target_name} currently link (after C-namespace
# symbol localization, see contrib/corrosion-cmake) and publishes the stripped
# archive as its replacement, as an ordinary input -> output build step.
#
# Skip in tidy builds where CMAKE_AR is a dummy script that doesn't extract archives.
# Skip in sanitizer builds where instrumentation adds special symbols (e.g.,
# ___asan_globals_registered) that the partial link + strip process breaks.
# Skip on Darwin where objects are Mach-O (ld.lld -r only handles ELF).
# Skip on ppc64le where ld.lld -r produces broken .eh_frame relocations.
function(clickhouse_strip_rust_symbols target_name)
    if (CMAKE_AR MATCHES "dummy_compiler_linker" OR SANITIZE OR OS_DARWIN
        OR CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        return()
    endif()
    # Use ld.lld for partial linking — it's a cross-linker that handles all ELF architectures,
    # unlike the host ld which may not support the target architecture.
    find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (NOT _LLD_FOR_STRIP)
        return()
    endif()

    set(_script "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh")
    clickhouse_get_rust_archive(${target_name} _input _input_deps)
    get_filename_component(_input_dir "${_input}" DIRECTORY)
    get_filename_component(_input_stem "${_input}" NAME_WE)
    set(_output "${_input_dir}/${_input_stem}-stripped.a")
    add_custom_command(
        OUTPUT "${_output}"
        COMMAND bash "${_script}"
            "${_input}" "${_output}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
            ${ARGN}
        DEPENDS ${_input_deps} "${_script}"
        COMMENT "Stripping internal symbols from ${target_name}"
        VERBATIM
    )
    clickhouse_set_rust_archive(${target_name} strip_rust_symbols_${target_name} "${_output}")
endfunction()

if (ENABLE_PRQL)
    clickhouse_config_crate_flags(_ch_rust_prql)
    clickhouse_strip_rust_symbols(_ch_rust_prql prql_to_sql prql_free_pointer)
    target_include_directories(_ch_rust_prql INTERFACE prql/include)
    add_library(ch_rust::prql ALIAS _ch_rust_prql)
endif()

if (ENABLE_POLYGLOT)
    clickhouse_config_crate_flags(_ch_rust_polyglot)
    clickhouse_strip_rust_symbols(_ch_rust_polyglot polyglot_transpile polyglot_free_pointer)
    target_include_directories(_ch_rust_polyglot INTERFACE polyglot/include)
    add_library(ch_rust::polyglot ALIAS _ch_rust_polyglot)
endif()

if (ENABLE_SKIM)
    clickhouse_config_crate_flags(_ch_rust_skim_rust)
    target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
    add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
endif()

add_subdirectory (wasmtime)
