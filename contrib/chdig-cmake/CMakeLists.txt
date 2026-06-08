set(ENABLE_CHDIG_DEFAULT ${ENABLE_RUST})
# SANITIZE            - Some issues with undefined symbols
# NO_ARMV81_OR_HIGHER - Disabled because ring assumes that Neon is available with ARM
# OR OS_DARWIN        - linking issues (__Z11CityHash128PKcm not found)
# OS_FREEBSD          - linking issues (undefined reference to `getrandom' and CityHash128())
if (ENABLE_CHDIG_DEFAULT AND (SANITIZE OR NO_ARMV81_OR_HIGHER OR OS_DARWIN OR OS_FREEBSD))
    message(STATUS "Environment is not compatible with chdig. Disabling.")
    set(ENABLE_CHDIG_DEFAULT OFF)
endif()
option(ENABLE_CHDIG "Use chdig" ${ENABLE_CHDIG_DEFAULT})

# Explicitly check for ENABLE_RUST one more time if it was enabled before, but now disabled, i.e.
#
#   cmake -DENABLE_RUST=1
#   cmake -DENABLE_RUST=0
if (NOT ENABLE_CHDIG OR NOT ENABLE_RUST)
    message(STATUS "Not using chdig")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH ../chdig/Cargo.toml ALL_FEATURES)
clickhouse_config_crate_flags(chdig)
add_library(ch_rust::chdig ALIAS chdig)
