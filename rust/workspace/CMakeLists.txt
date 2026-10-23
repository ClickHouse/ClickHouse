if (OS_FREEBSD)
    # Right nix/libc requires fspacectl and it had been added only since FreeBSD14.
    # And since sysroot has older libraries you will got undefined reference for clickhouse binary.
    #
    # But likely everything should work without this syscall, however it is not
    # possible right now to gently override libraries versions for dependencies,
    # and forking rust modules is a little bit too much for this thing.
    #
    # You can take a look at the details in the following issue [1].
    #
    #   [1]: https://github.com/rust-lang/cargo/issues/5640
    #
    # Update 2024-04: Now prql also requires getrandom() via std::sys::pal::unix::rand::imp::getrandom_fill_bytes
    message(STATUS "Rust build is disabled for FreeBSD because we use old sysroot files")
    return()
endif()

option (ENABLE_PRQL "Enable prql" ${ENABLE_LIBRARIES})
option (ENABLE_SKIM "Enable skim" ${ENABLE_LIBRARIES})
option (ENABLE_POLYGLOT "Enable polyglot SQL transpiler" ${ENABLE_LIBRARIES})
option (ENABLE_PCO "Enable pco (pcodec) compression codec" ${ENABLE_LIBRARIES})
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

if (NOT ENABLE_PRQL AND NOT ENABLE_SKIM AND NOT ENABLE_POLYGLOT AND NOT ENABLE_PCO AND NOT ENABLE_LANCE)
    message(STATUS "prql, skim, polyglot, pco, and lance are disabled")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

if (ENABLE_PRQL)
    clickhouse_config_crate_flags(_ch_rust_prql)
    target_include_directories(_ch_rust_prql INTERFACE prql/include)
    add_library(ch_rust::prql ALIAS _ch_rust_prql)

    # Strip internal symbols from the PRQL static library after it is built.
    # Only the C FFI functions need to remain visible; all other symbols (67K+)
    # from the chumsky parser combinator are internal and produce enormous names.
    # Skip in tidy builds where CMAKE_AR is a dummy script that doesn't extract archives.
    # Skip in sanitizer builds where instrumentation adds special symbols (e.g.,
    # ___asan_globals_registered) that the partial link + strip process breaks.
    # Skip on Darwin where objects are Mach-O (ld.lld -r only handles ELF).
    # Skip on ppc64le where ld.lld -r produces broken .eh_frame relocations.
    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        # Use ld.lld for partial linking — it's a cross-linker that handles all ELF architectures,
        # unlike the host ld which may not support the target architecture.
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_prql_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_prql.a")
            add_custom_target(strip_prql_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_prql_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    prql_to_sql prql_free_pointer
                DEPENDS cargo-build__ch_rust_prql
                COMMENT "Stripping internal symbols from PRQL library"
                VERBATIM
            )
            add_dependencies(_ch_rust_prql strip_prql_symbols)
            # Ensure localize runs before strip (they both modify the same .a)
            if (TARGET localize_rust_c__ch_rust_prql)
                add_dependencies(strip_prql_symbols localize_rust_c__ch_rust_prql)
            endif()
        endif ()
    endif ()
endif()

if (ENABLE_POLYGLOT)
    clickhouse_config_crate_flags(_ch_rust_polyglot)
    target_include_directories(_ch_rust_polyglot INTERFACE polyglot/include)
    add_library(ch_rust::polyglot ALIAS _ch_rust_polyglot)

    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_polyglot_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_polyglot.a")
            add_custom_target(strip_polyglot_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_polyglot_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    polyglot_transpile polyglot_free_pointer
                DEPENDS cargo-build__ch_rust_polyglot
                COMMENT "Stripping internal symbols from polyglot library"
                VERBATIM
            )
            add_dependencies(_ch_rust_polyglot strip_polyglot_symbols)
            # Ensure localize runs before strip (they both modify the same .a)
            if (TARGET localize_rust_c__ch_rust_polyglot)
                add_dependencies(strip_polyglot_symbols localize_rust_c__ch_rust_polyglot)
            endif()
        endif ()
    endif ()
endif()

if (ENABLE_PCO)
    clickhouse_config_crate_flags(_ch_rust_pco)
    target_include_directories(_ch_rust_pco INTERFACE pco/include)
    add_library(ch_rust::pco ALIAS _ch_rust_pco)

    # Strip internal symbols from the pco static library; only the two C FFI
    # entry points need to stay visible (the pco crate and its dependencies
    # export a large number of internal symbols). Same guards as prql/polyglot.
    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_pco_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_pco.a")
            add_custom_target(strip_pco_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_pco_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    pco_compress pco_decompress
                DEPENDS cargo-build__ch_rust_pco
                COMMENT "Stripping internal symbols from pco library"
                VERBATIM
            )
            add_dependencies(_ch_rust_pco strip_pco_symbols)
            # Ensure localize runs before strip (they both modify the same .a)
            if (TARGET localize_rust_c__ch_rust_pco)
                add_dependencies(strip_pco_symbols localize_rust_c__ch_rust_pco)
            endif()
        endif ()
    endif ()
endif()

if (ENABLE_SKIM)
    clickhouse_config_crate_flags(_ch_rust_skim_rust)
    target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
    add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
endif()

if (ENABLE_LANCE)
    clickhouse_config_crate_flags(_ch_rust_lance)
    target_include_directories(_ch_rust_lance INTERFACE lance/include)
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
    # (including the Lance crates) uses our copies.
    if (TARGET localize_rust_c__ch_rust_lance AND TARGET _lz4 AND TARGET _zstd)
        get_target_property(_lance_lib _ch_rust_lance IMPORTED_LOCATION)
        if (NOT _lance_lib OR _lance_lib MATCHES "NOTFOUND")
            set(_lance_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_lance.a")
        endif()
        add_custom_target(localize_rust_contrib_symbols__ch_rust_lance
            COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/localize_rust_c_symbols.sh"
                "${_lance_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${NM_PATH}"
                "$<TARGET_FILE:_lz4>" "$<TARGET_FILE:_zstd>"
            COMMENT "Localizing lz4 and zstd symbols in _ch_rust_lance"
            VERBATIM
        )
        # Both targets rewrite the same archive, so they must not run concurrently.
        add_dependencies(localize_rust_contrib_symbols__ch_rust_lance
            localize_rust_c__ch_rust_lance _lz4 _zstd)
        add_dependencies(_ch_rust_lance localize_rust_contrib_symbols__ch_rust_lance)
    endif()
endif()

add_subdirectory (wasmtime)
