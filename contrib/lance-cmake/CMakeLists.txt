option (ENABLE_LANCE "Enable Lance DataLake adapter" ${ENABLE_LIBRARIES})

# The Lance crates depend on `ring` (through `object_store` and `rustls`), the same as `delta-kernel-rs`:
# - MSan: `ring` disables its assembly under MSan, but still calls the assembly functions, so the link fails.
# - NO_ARMV81_OR_HIGHER: `ring` assumes that Neon is available on ARM.
if (ENABLE_LANCE AND ((SANITIZE STREQUAL "memory") OR NO_ARMV81_OR_HIGHER))
    message (STATUS "Disabling Lance because the `ring` crate does not support this build")
    set (ENABLE_LANCE OFF)
endif()

# `lance-linalg` builds its C kernels only for x86_64, aarch64 and loongarch64.
if (ENABLE_LANCE AND NOT (ARCH_AMD64 OR ARCH_AARCH64))
    message (STATUS "Disabling Lance because `lance-linalg` does not support this architecture")
    set (ENABLE_LANCE OFF)
endif()

# The fuzzers do not exercise Lance, and its crates push the ASan fuzzer binary over the size limit of
# 32-bit PC-relative relocations.
if (ENABLE_LANCE AND ENABLE_FUZZING)
    message (STATUS "Disabling Lance in the fuzzer build")
    set (ENABLE_LANCE OFF)
endif()

# The Lance crates triple the DWARF unwind info (`__eh_frame`) of the Darwin binary, pushing it past 16 MiB.
# Compact-unwind entries that defer to DWARF store the FDE offset in 24 bits, so the offsets of the FDEs past
# 16 MiB (including `__cxa_throw`) get truncated, and the unwinder falls back to a linear scan of all FDEs on
# every thrown exception, making exceptions thousands of times slower.
if (ENABLE_LANCE AND OS_DARWIN)
    message (STATUS "Disabling Lance on macOS")
    set (ENABLE_LANCE OFF)
endif()

# The Lance crates generate code from `.proto` files at build time, which needs the `protoc` built
# from `contrib/google-protobuf`.
if (ENABLE_LANCE AND NOT TARGET protoc)
    message (STATUS "Disabling Lance because Protobuf is disabled")
    set (ENABLE_LANCE OFF)
endif()

if (NOT ENABLE_LANCE)
    message(STATUS "Not using lance")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" CRATES _ch_rust_lance)
clickhouse_config_crate_flags(_ch_rust_lance)
target_include_directories(_ch_rust_lance INTERFACE include)
add_library(ch_rust::lance ALIAS _ch_rust_lance)

# The Lance crates generate Rust code from `.proto` files with `prost-build`, which needs a
# `protoc` binary at build time. Point it at the one ClickHouse builds from
# `contrib/google-protobuf` so the build does not silently depend on a host installation.
# When cross-compiling, `protoc` comes from the nested native build, the same way
# `protobuf_generate` uses it.
if (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR OR USE_MUSL)
    set (_lance_protoc "${PROJECT_BINARY_DIR}/native/contrib/google-protobuf-cmake/protoc")
else ()
    set (_lance_protoc "$<TARGET_FILE:protoc>")
    # `contrib` is added with `EXCLUDE_FROM_ALL`, so `protoc` is only built when something asks for it.
    add_dependencies (cargo-build__ch_rust_lance protoc)
endif()
corrosion_set_env_vars(_ch_rust_lance "PROTOC=${_lance_protoc}")
# `prost-build` only ships the well-known types (`google/protobuf/empty.proto` and friends)
# alongside its own vendored `protoc`, so an external one has to be told where to find them.
corrosion_set_env_vars(_ch_rust_lance "PROTOC_INCLUDE=${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")

# The Lance crates bundle their own copies of lz4 (`lz4-sys`) and zstd (`zstd-sys`), which can win
# over `contrib/lz4` and `contrib/zstd` at link time depending on the link order. Localize every
# symbol that the Lance archive defines and our contrib libraries also define, so that the binary
# (including the Lance crates) uses our copies. The guard is the same as for the localization step
# that `clickhouse_config_crate_flags` adds.
if (TARGET localize_rust_c__ch_rust_lance AND TARGET _lz4 AND TARGET _zstd)
    clickhouse_postprocess_rust_lib(_ch_rust_lance
        STEP localize_rust_contrib_symbols
        SCRIPT "${ClickHouse_SOURCE_DIR}/cmake/localize_rust_c_symbols.sh"
        ARGS "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${NM_PATH}" "$<TARGET_FILE:_lz4>" "$<TARGET_FILE:_zstd>"
        DEPENDS _lz4 _zstd
        COMMENT "Localizing lz4 and zstd symbols in _ch_rust_lance"
    )
endif()

# Then keep only the FFI entry points of the Lance static library global, the same as for Vortex.
#
# The localization above did not take effect in the ARM release build (there the C code of the crates
# goes through ThinLTO, so it is likely LLVM bitcode, which `llvm-objcopy` does not rewrite): the
# bundled zstd 1.5.7 won over `contrib/zstd`, and the whole server used it. The `ZSTD` codec then
# lacked the NEON overlap copy of `contrib/zstd`, which made reading `ZSTD`-compressed columns about
# 2x slower on ARM (`vectorize_aggregation_combinators` in the performance tests). A partial link
# handles bitcode.
if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    ch_find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        # Every exported function is declared as `ch_lance_<name>(` in the FFI header, which stays
        # the only place that defines the ABI surface. Comment lines are skipped.
        set(_lance_ffi_header "${CMAKE_CURRENT_SOURCE_DIR}/include/ch_lance.h")
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_lance_ffi_header}")

        file(STRINGS "${_lance_ffi_header}" _lance_ffi_declarations REGEX "ch_lance_[a-z0-9_]+[ \t]*\\(")
        set(_lance_ffi_symbols "")
        foreach (_declaration IN LISTS _lance_ffi_declarations)
            if (_declaration MATCHES "^[ \t]*(//|\\*)")
                continue()
            endif()
            string(REGEX MATCHALL "ch_lance_[a-z0-9_]+[ \t]*\\(" _matches "${_declaration}")
            foreach (_match IN LISTS _matches)
                string(REGEX REPLACE "[ \t]*\\($" "" _symbol "${_match}")
                list(APPEND _lance_ffi_symbols "${_symbol}")
            endforeach()
        endforeach()
        list(REMOVE_DUPLICATES _lance_ffi_symbols)

        # Fail closed: an empty keep list would leave nothing to link against.
        if (NOT _lance_ffi_symbols)
            message(FATAL_ERROR "No ch_lance_* entry points found in ${_lance_ffi_header}. "
                "Either the header moved or its declaration style changed; "
                "stripping the library against an empty keep list would leave nothing to link against.")
        endif()

        list(LENGTH _lance_ffi_symbols _lance_ffi_symbol_count)
        message(STATUS "Lance: keeping ${_lance_ffi_symbol_count} FFI entry points when stripping")

        clickhouse_postprocess_rust_lib(_ch_rust_lance
            STEP strip_rust_symbols
            SCRIPT "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
            ARGS "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}" ${_lance_ffi_symbols}
            COMMENT "Stripping internal symbols from Lance library"
        )
    endif()
endif()
