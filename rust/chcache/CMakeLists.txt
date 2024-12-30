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
        RUSTFLAGS=
        RUST_CFLAGS=
        RUST_CXXFLAGS=
        CFLAGS=
        CXXFLAGS=
    )
    corrosion_set_hostbuild(chcache)
endif()
