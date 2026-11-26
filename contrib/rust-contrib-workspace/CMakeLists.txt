# The one static library of the Rust workspace: the root package re-exports every enabled member (see `src/lib.rs`), so
# one cargo run compiles them all in parallel and one archive holds them, with the standard library and the crates they
# share linked once. The members only add the interface targets that link it (`clickhouse_add_rust_member`).
get_property(_features GLOBAL PROPERTY CLICKHOUSE_RUST_FEATURES)
if (NOT _features)
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH "${RUST_CONTRIB_WORKSPACE_MANIFEST}" FEATURES ${_features})
clickhouse_config_crate_flags(_ch_rust_workspace)
# For `zstd-sys` and `lz4-sys`, which do not bundle copies of these libraries (see `config.toml.in`).
# `LINK_ONLY`: the consumers of the workspace must not get the include directories of these libraries. The one of LZ4
# contains a legacy `xxhash.h`, which would shadow the one of `ch_contrib::xxHash` for the C++ code.
target_link_libraries(_ch_rust_workspace INTERFACE $<LINK_ONLY:ch_contrib::zstd> $<LINK_ONLY:ch_contrib::lz4>)

# The build script of `delta_kernel_ffi` writes `delta_kernel_ffi.hpp` only when it runs and does not track all the crates
# the header is generated from, so the header must not come from a shared Cargo build directory.
if (RUST_BUILD_CACHE_DIR AND "delta_kernel" IN_LIST _features)
    corrosion_set_cargo_flags(_ch_rust_workspace "--config=build.build-dir='${CMAKE_CURRENT_BINARY_DIR}/cargo-build-dir'")
    message(STATUS "RUST_BUILD_CACHE_DIR is not used: the Rust workspace includes delta-kernel-rs")
endif()

get_property(_build_depends GLOBAL PROPERTY CLICKHOUSE_RUST_BUILD_DEPENDS)
if (_build_depends)
    list(REMOVE_DUPLICATES _build_depends)
    add_dependencies(_cargo-build__ch_rust_workspace ${_build_depends})
endif()

# Strip the internal symbols of the library after it is built.
#
# The crates define hundreds of thousands of global Rust symbols (268K for Vortex alone, 19 MiB of names; the parser
# combinator of PRQL produces enormous ones). None of them is callable from C++: the entry points are C functions and
# the C++ code of `cxx`, so the script localizes and drops the Rust symbols and keeps everything else, except the C
# libraries that `-sys` crates bundle (`ring`, ...) and `compiler_builtins`: in the one object of the partial link,
# they would be duplicates of the copies ClickHouse links.
#
# Note: `clickhouse-stripped` drops the debug info but keeps `.symtab` and `.strtab`.
#
# Skip in tidy builds where CMAKE_AR is a dummy script that doesn't extract archives.
# Skip in sanitizer builds where instrumentation adds special symbols (e.g.,
# ___asan_globals_registered) that the partial link + strip process breaks.
# Skip on Darwin where objects are Mach-O (ld.lld -r only handles ELF).
# Skip on ppc64le where ld.lld -r produces broken .eh_frame relocations.
# Skip with ENABLE_CFI, where the library is LLVM bitcode: the partial link would run LTO and lower the CFI checks of
# these objects on their own, apart from the rest of the program.
if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT ENABLE_CFI AND NOT OS_DARWIN
    AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
    # Use ld.lld for partial linking — it's a cross-linker that handles all ELF architectures,
    # unlike the host ld which may not support the target architecture.
    ch_find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    if (_LLD_FOR_STRIP)
        clickhouse_postprocess_rust_lib(_ch_rust_workspace
            STEP strip_rust_symbols
            SCRIPT "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
            ARGS "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}" "${NM_PATH}"
            COMMENT "Stripping internal symbols from the Rust library"
        )
    endif ()
endif ()
