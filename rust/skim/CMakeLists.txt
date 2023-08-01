if (OS_FREEBSD)
    # Right nix/libc requires fspacectl and it had been added only since FreeBSD14.
    # And sicne sysroot has older libararies you will got undefined reference for clickhouse binary.
    #
    # But likely everything should work without this syscall, however it is not
    # possible right now to gently override libraries versions for depdendcies,
    # and forking rust modules is a little bit too much for this thing.
    #
    # You can take a look at the details in the fillowing issue [1].
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

set (ffi_binding_generated_path
     ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/cargo/build/${Rust_CARGO_TARGET_CACHED}/cxxbridge/_ch_rust_skim_rust/src/lib.rs.cc)
set (ffi_binding_final_path ${CMAKE_CURRENT_BINARY_DIR}/skim-ffi.cc)
message(STATUS "Writing FFI Binding for skim: ${ffi_binding_generated_path} => ${ffi_binding_final_path}")

add_custom_command(OUTPUT ${ffi_binding_final_path}
    COMMAND ${CMAKE_COMMAND} -E copy ${ffi_binding_generated_path} ${ffi_binding_final_path}
    DEPENDS cargo-build__ch_rust_skim_rust)

add_library(_ch_rust_skim_ffi ${ffi_binding_final_path})

# cxx bridge compiles such bindings
set_target_properties(_ch_rust_skim_ffi PROPERTIES COMPILE_FLAGS "${CXXBRIDGE_CXXFLAGS}")

add_library(_ch_rust_skim INTERFACE)
target_include_directories(_ch_rust_skim INTERFACE include)
target_link_libraries(_ch_rust_skim INTERFACE
    _ch_rust_skim_rust
    _ch_rust_skim_ffi)

add_library(ch_rust::skim ALIAS _ch_rust_skim)
