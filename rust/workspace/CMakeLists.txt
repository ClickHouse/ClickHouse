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

if (NOT ENABLE_PRQL AND NOT ENABLE_SKIM)
    message(STATUS "prql and skim are disabled")
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
    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker")
        set(_prql_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_prql.a")
        add_custom_target(strip_prql_symbols
            COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                "${_prql_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${CMAKE_LINKER}"
                prql_to_sql prql_free_pointer
            DEPENDS cargo-build__ch_rust_prql
            COMMENT "Stripping internal symbols from PRQL library"
            VERBATIM
        )
        add_dependencies(_ch_rust_prql strip_prql_symbols)
    endif ()
endif()

if (ENABLE_SKIM)
    clickhouse_config_crate_flags(_ch_rust_skim_rust)
    target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
    add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
endif()

add_subdirectory (wasmtime)
