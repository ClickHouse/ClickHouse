if (COMPILER_CACHE STREQUAL "chcache")
    configure_file(".cargo/config.toml.in" ".cargo/config.toml" @ONLY)

    corrosion_import_crate(
        MANIFEST_PATH Cargo.toml
        PROFILE release
        LOCKED
        FLAGS --offline
    )

    corrosion_set_env_vars(
        chcache
        "CARGO_HOME=${CMAKE_CURRENT_BINARY_DIR}/chcache_home"
        RUSTFLAGS=
        RUST_CFLAGS=
        RUST_CXXFLAGS=
        CFLAGS=
        CXXFLAGS=
    )

    get_property(
            INTERFACE_CCACHE_CARGO_FLAGS
            TARGET delta_kernel_ffi
            PROPERTY INTERFACE_CORROSION_CARGO_FLAGS
    )
    set_property(
            TARGET delta_kernel_ffi
            PROPERTY INTERFACE_CORROSION_CARGO_FLAGS "${INTERFACE_CCACHE_CARGO_FLAGS};--config=${RUST_CONFIG_FILE}"
    )
    get_property(
            INTERFACE_CCACHE_CARGO_FLAGS
            TARGET delta_kernel_ffi
            PROPERTY INTERFACE_CORROSION_CARGO_FLAGS
    )
    message(STATUS "chcache cargo flags: ${INTERFACE_CCACHE_CARGO_FLAGS}")

    corrosion_set_hostbuild(chcache)
endif()
