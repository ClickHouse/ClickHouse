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
    message(STATUS "skim is disabled for FreeBSD")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

# -Wno-dollar-in-identifier-extension: cxx bridge complies names with '$'
# -Wno-unused-macros: unused CXXBRIDGE1_RUST_STRING
set(CXXBRIDGE_CXXFLAGS "-Wno-dollar-in-identifier-extension -Wno-unused-macros")
set(RUST_CXXFLAGS "${RUST_CXXFLAGS} ${CXXBRIDGE_CXXFLAGS}")
message(STATUS "RUST_CXXFLAGS (for skim): ${RUST_CXXFLAGS}")
# NOTE: requires RW access for the source dir
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/build.rs.in" "${CMAKE_CURRENT_SOURCE_DIR}/build.rs" @ONLY)
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml" @ONLY)

target_include_directories(_ch_rust_skim_rust INTERFACE include)
add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
