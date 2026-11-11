# The members share one target directory, so their cargo runs take its lock in turn. One run of the root package, which
# depends on every enabled member, compiles them in parallel first; the run of each member then compiles only its crate.
set(_crates _ch_rust_skim_rust _ch_rust_prql _ch_rust_polyglot _ch_rust_vortex _ch_rust_pco _ch_rust_wasmtime _ch_rust_delta_kernel _ch_rust_chdig)
set(_features skim prql polyglot vortex pco wasmtime delta_kernel chdig)

set(_enabled_crates "")
set(_enabled_features "")
foreach (_crate _feature IN ZIP_LISTS _crates _features)
    if (TARGET cargo-build_${_crate})
        list(APPEND _enabled_crates ${_crate})
        list(APPEND _enabled_features ${_feature})
    endif()
endforeach()

# With LTO (the `release-thinlto` profile) the run of each member does the code generation of its whole dependency graph
# itself and recompiles a large part of what the root run built, so the root run would only add work.
if (NOT _enabled_crates OR (ENABLE_THINLTO AND NOT CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG"))
    return()
endif()

# The only package of the workspace that is static in its manifest; `CRATES` would only add a redundant `OVERRIDE_CRATE_TYPE`.
clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" FEATURES ${_enabled_features})
clickhouse_config_crate_env(_ch_rust_workspace)

# The root run builds the build scripts of the members as well, so it needs what their runs need (OpenSSL).
foreach (_crate IN LISTS _enabled_crates)
    add_dependencies(cargo-prebuild_${_crate} cargo-build__ch_rust_workspace)
    get_target_property(_deps _cargo-build_${_crate} MANUALLY_ADDED_DEPENDENCIES)
    foreach (_dep IN LISTS _deps)
        if (_dep AND NOT _dep MATCHES "^cargo-prebuild_")
            add_dependencies(_cargo-build__ch_rust_workspace ${_dep})
        endif()
    endforeach()
endforeach()
