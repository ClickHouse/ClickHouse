set(USE_CHDIG_DEFAULT ${ENABLE_RUST})
# SANITIZE            - Some issues with undefined symbols
# NO_ARMV81_OR_HIGHER - Disabled because ring assumes that Neon is available with ARM
# ENABLE_CLANG_TIDY   - clang-tidy knows nothing about Rust, plus chdig requires some libraries that are not generated due to dummy launcher (this can be fixed, but what for, let's simply disable it under tidy)
# OS_DARWIN           - Uses default linker (over lld)
# OS_FREEBSD          - Rust adds bunch of extra libraries as dependencies
# ARCH_PPC64LE        - Does not have Scrt1.o
if (USE_CHDIG_DEFAULT AND (SANITIZE OR NO_ARMV81_OR_HIGHER OR ENABLE_CLANG_TIDY OR OS_DARWIN OR OS_FREEBSD OR ARCH_PPC64LE))
    message(STATUS "Environment is not compatible with chdig. Disabling.")
    set(USE_CHDIG_DEFAULT OFF)
endif()
option(USE_CHDIG "Use chdig" ${USE_CHDIG_DEFAULT})

if (NOT USE_CHDIG)
    message(STATUS "Not using chdig")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH ../chdig/Cargo.toml ALL_FEATURES CRATE_TYPES bin)
clickhouse_config_crate_flags(chdig)

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
