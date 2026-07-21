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

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)
clickhouse_config_crate_flags(_ch_rust_prql)
clickhouse_config_crate_flags(_ch_rust_skim_rust)

target_include_directories(_ch_rust_prql INTERFACE prql/include)
add_library(ch_rust::prql ALIAS _ch_rust_prql)

target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)