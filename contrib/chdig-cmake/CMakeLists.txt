set(ENABLE_CHDIG_DEFAULT ${ENABLE_RUST})
# SANITIZE            - Some issues with undefined symbols
# OS_FREEBSD          - linking issues (undefined reference to `getrandom' and CityHash128())
if (ENABLE_CHDIG_DEFAULT AND (SANITIZE OR OS_FREEBSD))
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

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_chdig)
clickhouse_config_crate_flags(_ch_rust_chdig)
add_library(ch_rust::chdig ALIAS _ch_rust_chdig)

if (OS_DARWIN)
    target_link_libraries(_ch_rust_chdig INTERFACE "-framework AppKit")
endif()
