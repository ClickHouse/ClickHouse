set(USE_CHDIG_DEFAULT ${ENABLE_RUST})
if (USE_CHDIG_DEFAULT AND (SANITIZE OR NO_ARMV81_OR_HIGHER))
    message(STATUS "Environment is not compatible with chdig. Disabling.")
    set(USE_CHDIG_DEFAULT OFF)
endif()
option(USE_CHDIG "Use chdig" ${USE_CHDIG_DEFAULT})

if (NOT USE_CHDIG)
    message(STATUS "Not using chdig")
    return()
endif()

if (USE_CHDIG)
    clickhouse_import_crate(MANIFEST_PATH ../chdig/Cargo.toml ALL_FEATURES CRATE_TYPES bin)
    clickhouse_config_crate_flags(chdig)

    # See [1] for more details, but basically:
    # - we need to explicitly set CXXSTDLIB to avoid using of libstdc++
    # - link with the library explicitly to add search path (-L)
    #
    #   [1]: https://github.com/rust-lang/cc-rs/blob/7666d4f4f36d3bcacce9ab72fc6538fcc0ea1716/src/lib.rs#L762-L775
    corrosion_set_env_vars(chdig "CXXSTDLIB=cxx")
    corrosion_link_libraries(chdig cxx)

    corrosion_link_libraries(chdig unwind)

    # Add dummy libraries to satisfy dependencies for libc crate.
    # See [1] for more details.
    #
    #   [1]: https://github.com/rust-lang/libc/blob/5c0681084929c91c99b4d23245db2ac777cec390/src/unix/mod.rs#L391-L490
    add_library(util system.c)
    add_library(rt system.c)
    # But leave the following:
    # - c
    # - m
    # - dl
    # - pthread
    corrosion_link_libraries(chdig util rt)
    # This comes from a different place.
    add_library(gcc_s system.c)
    corrosion_link_libraries(chdig gcc_s)
endif()
