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
    message(STATUS "skim is disabled for FreeBSD because we use old sysroot files")
    return()
endif()

if (NOT ENABLE_RUST)
    message(STATUS "Not using skim (Rust is disabled)")
    return()
endif()

option (ENABLE_SKIM "Enable skim" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SKIM)
    message(STATUS "skim is disabled")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../skim/Cargo.toml)
clickhouse_config_crate_flags(_ch_rust_skim_rust)
target_include_directories(_ch_rust_skim_rust INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/../skim/include)
add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
