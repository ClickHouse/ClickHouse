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

# The Lance crates generate Rust code from `.proto` files with `prost-build`, which needs a
# `protoc` binary at build time. Point it at the one ClickHouse builds from
# `contrib/google-protobuf` so the build does not silently depend on a host installation.
# When cross-compiling, `protoc` comes from the nested native build, the same way
# `protobuf_generate` uses it.
set (_lance_protoc_depends "")
if (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR OR USE_MUSL)
    set (_lance_protoc "${PROJECT_BINARY_DIR}/native/contrib/google-protobuf-cmake/protoc")
else ()
    set (_lance_protoc "$<TARGET_FILE:protoc>")
    # `contrib` is added with `EXCLUDE_FROM_ALL`, so `protoc` is only built when something asks for it.
    set (_lance_protoc_depends protoc)
endif()

# The bundled copies of lz4 (`lz4-sys`) and zstd (`zstd-sys`) are localized, together with the Rust symbols, when the
# one static library of the workspace is stripped (see `contrib/rust-contrib-workspace`), so the binary, including the
# Lance crates, uses `contrib/lz4` and `contrib/zstd`.
clickhouse_add_rust_member(_ch_rust_lance FEATURE lance
    BUILD_DEPENDS ${_lance_protoc_depends}
    ENV
        "PROTOC=${_lance_protoc}"
        # `prost-build` only ships the well-known types (`google/protobuf/empty.proto` and friends)
        # alongside its own vendored `protoc`, so an external one has to be told where to find them.
        "PROTOC_INCLUDE=${ClickHouse_SOURCE_DIR}/contrib/google-protobuf/src")
target_include_directories(_ch_rust_lance INTERFACE include)
add_library(ch_rust::lance ALIAS _ch_rust_lance)
