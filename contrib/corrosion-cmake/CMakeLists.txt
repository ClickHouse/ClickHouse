# See https://doc.rust-lang.org/nightly/rustc/platform-support.html

macro(set_rust_target)
    if(DEFINED CMAKE_TOOLCHAIN_FILE AND NOT DEFINED Rust_CARGO_TARGET)
        if(CMAKE_TOOLCHAIN_FILE MATCHES "ppc64le")
            set(Rust_CARGO_TARGET "powerpc64le-unknown-linux-gnu" CACHE INTERNAL "Rust config")
        elseif((CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-x86_64") AND (CMAKE_TOOLCHAIN_FILE MATCHES "musl"))
            set(Rust_CARGO_TARGET "x86_64-unknown-linux-musl" CACHE INTERNAL "Rust config")
        elseif(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-x86_64")
            set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu" CACHE INTERNAL "Rust config")
        elseif((CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-aarch64") AND (CMAKE_TOOLCHAIN_FILE MATCHES "musl"))
            set(Rust_CARGO_TARGET "aarch64-unknown-linux-musl" CACHE INTERNAL "Rust config")
        elseif(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-aarch64")
            set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu" CACHE INTERNAL "Rust config")
        elseif((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
            set(Rust_CARGO_TARGET "x86_64-apple-darwin" CACHE INTERNAL "Rust config")
        elseif((CMAKE_TOOLCHAIN_FILE MATCHES "darwin") AND (CMAKE_TOOLCHAIN_FILE MATCHES "aarch64"))
            set(Rust_CARGO_TARGET "aarch64-apple-darwin" CACHE INTERNAL "Rust config")
        elseif((CMAKE_TOOLCHAIN_FILE MATCHES "freebsd") AND (CMAKE_TOOLCHAIN_FILE MATCHES "x86_64"))
            set(Rust_CARGO_TARGET "x86_64-unknown-freebsd" CACHE INTERNAL "Rust config")
        elseif(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-riscv64")
            set(Rust_CARGO_TARGET "riscv64gc-unknown-linux-gnu" CACHE INTERNAL "Rust config")
        elseif(CMAKE_TOOLCHAIN_FILE MATCHES "linux/toolchain-loongarch64")
            set(Rust_CARGO_TARGET "loongarch64-unknown-linux-gnu" CACHE INTERNAL "Rust config")
        else()
            message(FATAL_ERROR "Unknown rust target for toolchain ${CMAKE_TOOLCHAIN_FILE}")
        endif()
        message(STATUS "Set Rust target to ${Rust_CARGO_TARGET}")
    endif ()

    # FindRust.cmake
    set(RUST_MODULE_PATH "${ClickHouse_SOURCE_DIR}/contrib/corrosion/cmake")
    if (NOT "${RUST_MODULE_PATH}" IN_LIST "${CMAKE_MODULE_PATH}")
        list (APPEND CMAKE_MODULE_PATH "${RUST_MODULE_PATH}")
    endif()
endmacro()

# Keep in sync with toolchain in docker images
# (must be called before find_package(Rust) and inclusion of Corrosion.cmake)
set(Rust_TOOLCHAIN "nightly-2026-03-22")

macro(find_rust)
    set_rust_target()
    if (NOT DEFINED Rust_FOUND)
        find_package(Rust)
    endif()
    if (NOT Rust_FOUND)
        message(${RECONFIGURE_MESSAGE_LEVEL} "Cannot find ${Rust_TOOLCHAIN} Rust toolchain. You can install it with 'rustup toolchain install ${Rust_TOOLCHAIN}'")
    endif()
endmacro()

if (NOT ENABLE_LIBRARIES)
    set(DEFAULT_ENABLE_RUST FALSE)
elseif (NOT DEFINED ENABLE_RUST)
    find_rust()
    set(DEFAULT_ENABLE_RUST ${Rust_FOUND})
else()
    if (ENABLE_RUST)
        # Give a sensible warning if Rust is not available
        find_rust()
        set(DEFAULT_ENABLE_RUST TRUE)
    else()
        set(DEFAULT_ENABLE_RUST FALSE)
    endif()
endif()

option(ENABLE_RUST "Enable rust" ${DEFAULT_ENABLE_RUST})
if(NOT ENABLE_RUST)
    message(STATUS "Not using Rust")
    return()
endif()

set_rust_target()
# Define function corrosion_import_crate()
include ("${ClickHouse_SOURCE_DIR}/contrib/corrosion/cmake/Corrosion.cmake")

set(CXX_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libcxx/include")

# For musl, libc++ headers must come before musl's, but the toolchain puts musl includes
# into CMAKE_*_FLAGS. Strip them and re-add them after libc++ for CXXFLAGS.
if (USE_MUSL)
    string(REGEX REPLACE "-isystem [^ ]*musl[^ ]*" "" CMAKE_C_FLAGS_STRIPPED "${CMAKE_C_FLAGS}")
    string(REGEX REPLACE "-isystem [^ ]*musl[^ ]*" "" CMAKE_CXX_FLAGS_STRIPPED "${CMAKE_CXX_FLAGS}")

    set(MUSL_STUB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/musl-cmake/include")
    set(MUSL_GENERATED_DIR "${ClickHouse_BINARY_DIR}/contrib/musl-cmake/include")
    set(MUSL_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/musl/include")
    set(MUSL_ARCH_DIR "${ClickHouse_SOURCE_DIR}/contrib/musl/arch/${MUSL_ARCH}")
    set(MUSL_GENERIC_DIR "${ClickHouse_SOURCE_DIR}/contrib/musl/arch/generic")
    set(MUSL_KERNEL_HEADERS_DIR "${ClickHouse_BINARY_DIR}/contrib/musl-cmake/kernel-headers")

    # The regex above strips every "-isystem ...musl..." entry, so we must reconstruct the full
    # musl header surface that cmake/linux/toolchain-*-musl.cmake puts into CMAKE_*_FLAGS via
    # MUSL_INCLUDE_FLAGS. Keep this in sync with that toolchain: the stub headers (e.g.
    # execinfo.h) and the kernel-headers directory are part of the set and must not be dropped,
    # otherwise Cargo cc/cxx builds compile against a narrower header surface than the rest of
    # the tree. The order matches the toolchain's -isystem order.
    set(MUSL_C_INCLUDES "-isystem ${MUSL_STUB_INCLUDE_DIR} -isystem ${MUSL_GENERATED_DIR} -isystem ${MUSL_INCLUDE_DIR} -isystem ${MUSL_ARCH_DIR} -isystem ${MUSL_GENERIC_DIR} -isystem ${MUSL_KERNEL_HEADERS_DIR}")

    set(MUSL_CXX_INCLUDES "-isystem ${CXX_INCLUDE_DIR} -nostdinc++ ${MUSL_C_INCLUDES} -D_LIBCPP_HAS_MUSL_LIBC=1")

    # Mirror the per-build-type selection of the non-musl branch below so that Release and
    # MinSizeRel musl artifacts do not silently compile Cargo cc/cxx shims with RelWithDebInfo
    # optimisation/debug flags.
    if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
        set(RUST_CFLAGS "${CMAKE_C_FLAGS_STRIPPED} ${CMAKE_C_FLAGS_DEBUG} ${MUSL_C_INCLUDES}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS_STRIPPED} ${CMAKE_CXX_FLAGS_DEBUG} ${MUSL_CXX_INCLUDES}")
    elseif (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
        set(RUST_CFLAGS "${CMAKE_C_FLAGS_STRIPPED} ${CMAKE_C_FLAGS_RELEASE} ${MUSL_C_INCLUDES}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS_STRIPPED} ${CMAKE_CXX_FLAGS_RELEASE} ${MUSL_CXX_INCLUDES}")
    elseif (CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
        set(RUST_CFLAGS "${CMAKE_C_FLAGS_STRIPPED} ${CMAKE_C_FLAGS_MINSIZEREL} ${MUSL_C_INCLUDES}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS_STRIPPED} ${CMAKE_CXX_FLAGS_MINSIZEREL} ${MUSL_CXX_INCLUDES}")
    else ()
        set(RUST_CFLAGS "${CMAKE_C_FLAGS_STRIPPED} ${CMAKE_C_FLAGS_RELWITHDEBINFO} ${MUSL_C_INCLUDES}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS_STRIPPED} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${MUSL_CXX_INCLUDES}")
    endif()
else()
    if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
        set(RUST_CFLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_DEBUG}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG} -isystem ${CXX_INCLUDE_DIR} -nostdinc++")
    elseif (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
        set(RUST_CFLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_RELEASE}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE} -isystem ${CXX_INCLUDE_DIR} -nostdinc++")
    elseif (CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
        set(RUST_CFLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_MINSIZEREL}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_MINSIZEREL} -isystem ${CXX_INCLUDE_DIR} -nostdinc++")
    else ()
        set(RUST_CFLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_RELWITHDEBINFO}")
        set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -isystem ${CXX_INCLUDE_DIR} -nostdinc++")
    endif()
endif()

if (CMAKE_OSX_SYSROOT)
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -isysroot ${CMAKE_OSX_SYSROOT}")
    set(RUST_CFLAGS "${RUST_CFLAGS} -isysroot ${CMAKE_OSX_SYSROOT}")
elseif(CMAKE_SYSROOT)
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} --sysroot ${CMAKE_SYSROOT}")
    set(RUST_CFLAGS "${RUST_CFLAGS} --sysroot ${CMAKE_SYSROOT}")
endif()

if (CMAKE_OSX_DEPLOYMENT_TARGET)
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -mmacosx-version-min=${CMAKE_OSX_DEPLOYMENT_TARGET}")
    set(RUST_CFLAGS "${RUST_CFLAGS} -mmacosx-version-min=${CMAKE_OSX_DEPLOYMENT_TARGET}")
endif()

get_target_property(CXX_PUBLIC_DEFS cxx INTERFACE_COMPILE_DEFINITIONS)
if (CXX_PUBLIC_DEFS)
    list(TRANSFORM CXX_PUBLIC_DEFS PREPEND "-D")
    list(JOIN CXX_PUBLIC_DEFS " " CXX_PUBLIC_DEFS)
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} ${CXX_PUBLIC_DEFS}")
endif()

if(CCACHE_EXECUTABLE MATCHES "/chcache$" OR CCACHE_EXECUTABLE MATCHES "/sccache$")
    message(STATUS "Using RUSTC_WRAPPER: ${CCACHE_EXECUTABLE}")
    set(RUSTCWRAPPER "${CCACHE_EXECUTABLE}")
else()
    set(RUSTCWRAPPER "")
endif()

# Note that RUSTFLAGS_CPU is defined in cpu_features.cmake
set(RUSTFLAGS ${RUSTFLAGS_CPU})

# Otherwise /usr/bin/ld will not be able to link our openssl build (built with lld) to openssl rust wrapper (built ld)
if (LINKER_NAME MATCHES "lld")
    list(APPEND RUSTFLAGS "-C" "link-arg=-fuse-ld=lld")
endif()

if (CMAKE_OSX_SYSROOT)
    list(APPEND RUSTFLAGS "-C" "link-arg=-isysroot")
    list(APPEND RUSTFLAGS "-C" "link-arg=${CMAKE_OSX_SYSROOT}")
elseif(CMAKE_SYSROOT)
    list(APPEND RUSTFLAGS "-C" "link-arg=--sysroot")
    list(APPEND RUSTFLAGS "-C" "link-arg=${CMAKE_SYSROOT}")
endif()

if (TOOLCHAIN_PATH)
    list(APPEND RUSTFLAGS "-C" "link-arg=--gcc-toolchain=${TOOLCHAIN_PATH}")
endif()

set(RUST_CARGO_BUILD_STD "")
# For more info: https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html#memorysanitizer
if (SANITIZE STREQUAL "memory")
    set(RUST_CARGO_BUILD_STD "-Zbuild-std=std,panic_abort,core,alloc")
    list(APPEND RUSTFLAGS "-Zsanitizer=memory" "-Zsanitizer-memory-track-origins")
elseif (SANITIZE STREQUAL "thread")
    set(RUST_CARGO_BUILD_STD "-Zbuild-std=std,panic_abort,core,alloc")
    list(APPEND RUSTFLAGS "-Zsanitizer=thread")
elseif (SANITIZE STREQUAL "address" OR SANITIZE STREQUAL "address,undefined")
    # Rust has no UBSan, so the combined ASan+UBSan build applies only ASan. Besides the
    # instrumentation itself, `-Zsanitizer=address` arms wasmtime's `cfg(asan)` guard, preventing
    # a stray munmap of live memory at thread exit: https://github.com/bytecodealliance/wasmtime/issues/13857
    set(RUST_CARGO_BUILD_STD "-Zbuild-std=std,panic_abort,core,alloc")
    list(APPEND RUSTFLAGS "-Zsanitizer=address")
endif()

# Force `rustix` to use its libc backend under sanitizers.
# On Linux `rustix` defaults to the `linux_raw` backend, which issues `mmap`, `munmap` and
# other syscalls directly via the `syscall` instruction, bypassing libc. The sanitizers
# intercept these operations at the libc symbol boundary, so with the raw backend they never
# observe `rustix`'s memory mappings. For ThreadSanitizer this means the shadow memory of a
# region is not reset when `rustix` (used by `wasmtime` for compiled-code `mmap` allocations)
# maps or unmaps it, so a freshly mapped region inherits the stale shadow of whatever
# previously occupied that virtual address, producing false-positive data races. The same gap
# affects MemorySanitizer (freshly mapped memory looks uninitialized) and AddressSanitizer.
# `--cfg=rustix_use_libc` is the mechanism `rustix` provides for transitive consumers to select
# the libc backend; it also activates the `libc` dependency (already pinned in `Cargo.lock`).
if (NOT SANITIZE STREQUAL "")
    list(APPEND RUSTFLAGS "--cfg" "rustix_use_libc")
endif()

# Set metadata flag to force different hashes for different sanitizers
if (SANITIZE STREQUAL "")
    list(APPEND RUSTFLAGS "-C" "metadata=no-sanitizer")
else()
    list(APPEND RUSTFLAGS "-C" "metadata=with-sanitizer-${SANITIZE}")
endif()

# Use v0 symbol mangling for shorter symbol names.
# The legacy mangling produces extremely long symbols (up to 165 KB) from deeply nested
# generic types in parser combinator libraries like chumsky (used by prqlc).
# v0 mangling uses backreference compression for repeated type paths.
list(APPEND RUSTFLAGS "-C" "symbol-mangling-version=v0")

# Suppress warnings
list(APPEND RUSTFLAGS
    # delta-kernel-rs
    "-Adead_code"
    # skim
    "-Amismatched_lifetime_syntaxes"
    # wasmtime
    "-Aunused_parens -Aunused_imports"
)

list(JOIN RUSTFLAGS " " RUSTFLAGS)

message(STATUS "RUST_CFLAGS: ${RUST_CFLAGS}")
message(STATUS "RUST_CXXFLAGS: ${RUST_CXXFLAGS}")
message(STATUS "RUSTFLAGS: ${RUSTFLAGS}")
message(STATUS "RUST_CARGO_BUILD_STD: ${RUST_CARGO_BUILD_STD}")

set(RUST_VENDOR_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../rust_vendor")

# Sanitizer instrumentation combined with opt-level=3 and the default codegen-units=16
# causes LLVM to use too much memory while compiling large crates like cranelift-codegen
# and polyglot-sql (with all-dialects). Set opt-level and codegen-units via the Cargo
# profile so that there is no conflict with the default profile flags.
if (NOT SANITIZE STREQUAL "")
    set(RUST_RELEASE_PROFILE_EXTRA "opt-level = 0
codegen-units = 4")
else()
    set(RUST_RELEASE_PROFILE_EXTRA "")
endif()

# The vendored Rust crates otherwise build with cargo's default dev profile (full DWARF debug=2 +
# incremental). debug=1 is cargo's "limited" level: line tables plus function/scope info - so Rust
# backtraces still resolve file:line - but no type or variable-level info; and dropping the
# incremental cache stops target/debug/incremental growing unbounded across rebuilds. On by default
# to keep Debug build dirs lean; set -DREDUCE_RUST_DEBUG_INFO=OFF for full DWARF + incremental when
# stepping into the crates. Affects Debug only (Release uses profile.release, where this is inert).
option(REDUCE_RUST_DEBUG_INFO "Build vendored Rust crates with reduced debug info (cargo debug=1, the \"limited\" level) and no incremental caches" ON)
if (REDUCE_RUST_DEBUG_INFO)
    set(RUST_DEV_PROFILE "[profile.dev]
debug = 1
incremental = false
")
else()
    set(RUST_DEV_PROFILE "")
endif()

# We now prepare a custom config.toml with all the flags and add it to cargo via flags
configure_file("config.toml.in" "config.toml" @ONLY)
set(RUST_CONFIG_FILE "${CMAKE_CURRENT_BINARY_DIR}/config.toml" CACHE INTERNAL "Rust config")
set(RUST_CFLAGS ${RUST_CFLAGS} CACHE INTERNAL "Rust config")
set(RUST_CXXFLAGS ${RUST_CXXFLAGS} CACHE INTERNAL "Rust config")
set(RUSTFLAGS "${RUSTFLAGS}" CACHE INTERNAL "Rust config")
set(RUSTCWRAPPER ${RUSTCWRAPPER} CACHE INTERNAL "Rust config")
set(RUST_CARGO_BUILD_STD ${RUST_CARGO_BUILD_STD} CACHE INTERNAL "Rust config")

function(clickhouse_import_crate)
    if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
        set(profile "")
    else()
        if (ENABLE_THINLTO)
            set(profile "release-thinlto")
        else()
            set(profile "release")
        endif()
    endif()

    disable_dummy_launchers_if_needed()
    corrosion_import_crate(${ARGN}
            # We set manually the cargo config flags via command line `--config` option
            # We have to do it this way because workspaces are built from source folder and we can't place the `config.toml`
            # there, as it would conflict if you have multiple build folders
            FLAGS "--config=${RUST_CONFIG_FILE}"
            NO_STD
            PROFILE ${profile}
            # We only care about static libraries and want to avoid as much dynamic libs as possible
            CRATE_TYPES staticlib
            LOCKED
            FLAGS --offline
            NO_DEFAULT_FEATURES
    )
    enable_dummy_launchers_if_needed()
endfunction()

# Configure a Rust crate target with ClickHouse-specific flags.
function(clickhouse_config_crate_flags target_name)
    corrosion_set_env_vars(${target_name} "CFLAGS=${RUST_CFLAGS}")
    corrosion_set_env_vars(${target_name} "CXXFLAGS=${RUST_CXXFLAGS}")
    corrosion_set_env_vars(${target_name} "RUSTFLAGS=${RUSTFLAGS}")
    corrosion_set_env_vars(${target_name} "RUSTDOCFLAGS=${RUSTFLAGS}")
    if (CMAKE_OSX_SYSROOT)
        corrosion_set_env_vars(${target_name} "SDKROOT=${CMAKE_OSX_SYSROOT}")
    endif()

    # sccache seems to be broken and mixes up builds
    if (NOT "${RUSTCWRAPPER}" STREQUAL "")
       corrosion_set_env_vars(${target_name} "CARGO_BUILD_RUSTC_WRAPPER=${RUSTCWRAPPER}")
    endif()

    if (NOT "${RUST_CARGO_BUILD_STD}" STREQUAL "")
        corrosion_set_cargo_flags(${target_name} "${RUST_CARGO_BUILD_STD}")
    endif()

    # See [1] for more details, but basically:
    # - we need to explicitly set CXXSTDLIB to avoid using of libstdc++
    # - link with the library explicitly to add search path (-L)
    #
    #   [1]: https://github.com/rust-lang/cc-rs/blob/7666d4f4f36d3bcacce9ab72fc6538fcc0ea1716/src/lib.rs#L762-L775
    corrosion_set_env_vars(${target_name} "CXXSTDLIB=cxx")
    # Even with --offline it is still possible that some commands from build.rs
    # will try to download, force them not to with CARGO_NET_OFFLINE=true
    #
    # Refs: https://github.com/rust-lang/cargo/issues/15099#issuecomment-2732847355
    corrosion_set_env_vars(${target_name} "CARGO_NET_OFFLINE=true")
    corrosion_link_libraries(${target_name} cxx)

    # Localize all C-namespace symbols from Rust static libraries so they
    # don't shadow our own implementations (libllvmlibc, glibc-compat).
    # Rust's compiler_builtins exports cbrt, fma, sin, etc. as global symbols;
    # without this, they silently win at link time and bypass our -falign-functions=64
    # and -march flags, causing performance regressions.
    #
    # For workspace crates the .a may land in a parent directory rather than
    # CMAKE_CURRENT_BINARY_DIR.  Set IMPORTED_LOCATION on the target before
    # calling this function if the default path is wrong (see wasmtime for an
    # example).
    get_target_property(_rust_lib_path ${target_name} IMPORTED_LOCATION)
    if (NOT _rust_lib_path OR _rust_lib_path MATCHES "NOTFOUND")
        set(_rust_lib_path "${CMAKE_CURRENT_BINARY_DIR}/lib${target_name}.a")
    endif()

    # Skip on Darwin (Mach-O), ppc64le (broken .eh_frame), and dummy builds.
    if (CMAKE_OBJCOPY AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le"
        AND NOT CMAKE_AR MATCHES "dummy_compiler_linker")
        # Verify the library path looks plausible at configure time.  The file
        # won't exist yet (cargo hasn't run), but the parent directory must exist
        # and the filename must end in .a — otherwise someone forgot to set
        # IMPORTED_LOCATION for a workspace subcrate.
        get_filename_component(_rust_lib_dir "${_rust_lib_path}" DIRECTORY)
        if (NOT IS_DIRECTORY "${_rust_lib_dir}")
            message(FATAL_ERROR
                "localize_rust_c_symbols: output directory '${_rust_lib_dir}' for "
                "target '${target_name}' does not exist.  If this is a workspace "
                "subcrate, set IMPORTED_LOCATION on the target before calling "
                "clickhouse_config_crate_flags().  Expected library: ${_rust_lib_path}")
        endif()

        # Reference libraries that define the C symbols Rust bundles.  The script
        # localizes exactly the symbols defined by both these libraries and the
        # Rust archive, so the binary's own copies win at link time.
        #
        # libllvmlibc (our optimized libm, AMD64/AARCH64 only) and clang_rt_builtins
        # (compiler-rt, every ELF target) are contrib targets, defined before any
        # caller of this function.
        set(_localize_ref_libs "")
        set(_localize_ref_deps "")
        foreach (_ref_target libllvmlibc clang_rt_builtins)
            if (TARGET ${_ref_target})
                list(APPEND _localize_ref_libs "$<TARGET_FILE:${_ref_target}>")
                list(APPEND _localize_ref_deps ${_ref_target})
            endif()
        endforeach()

        # Platform math provider: the library supplying the libm functions
        # libllvmlibc lacks (round, copysign, the ceil/rint/trunc IFUNCs, ...) and
        # the only such provider on arches without libllvmlibc.
        if (USE_MUSL)
            # musl has no separate libm: the math lives in libc, and libc is the
            # in-tree `musl` target built from sources (contrib/musl-cmake) — the
            # -static link uses it instead of a sysroot libc.a / -lc (see
            # cmake/musl.cmake and cmake/linux/default_libs.cmake).  Probing the
            # toolchain with -print-file-name would find a glibc library or
            # nothing, so use the built archive itself as the reference and add a
            # build dependency on it.
            list(APPEND _localize_ref_libs "$<TARGET_FILE:musl>")
            list(APPEND _localize_ref_deps musl)
        else()
            # Glibc keeps the math in libm (dynamic, -lm).  It is a prebuilt
            # sysroot/toolchain file, so no build dependency is needed.
            #
            # Resolve it once with the same constraints the toolchain compiles
            # with (--target/--sysroot/--gcc-toolchain/--no-default-config);
            # otherwise -print-file-name can return a host multiarch library.
            # Reject any result outside the sysroot/toolchain tree, or one
            # defining no symbols (an empty stub): a wrong or empty reference
            # would leave Rust's libm exports global and shadow the platform's.
            if (NOT DEFINED CACHE{CH_RUST_LOCALIZE_MATHLIB})
                set(_math_probe_flags "")
                set(_math_roots "")
                if (CMAKE_C_COMPILER_TARGET)
                    list(APPEND _math_probe_flags "--target=${CMAKE_C_COMPILER_TARGET}")
                endif()
                if (CMAKE_SYSROOT)
                    list(APPEND _math_probe_flags "--sysroot=${CMAKE_SYSROOT}")
                    list(APPEND _math_roots "${CMAKE_SYSROOT}")
                endif()
                if (TOOLCHAIN_PATH)
                    list(APPEND _math_probe_flags "--gcc-toolchain=${TOOLCHAIN_PATH}" "--no-default-config")
                    list(APPEND _math_roots "${TOOLCHAIN_PATH}")
                endif()

                set(_found_math "")
                foreach (_math_name libm.so.6 libm.a libm.so)
                    execute_process(
                        COMMAND "${CMAKE_C_COMPILER}" ${_math_probe_flags} "-print-file-name=${_math_name}"
                        OUTPUT_VARIABLE _math_out OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
                    if (NOT _math_out OR NOT EXISTS "${_math_out}")
                        continue()
                    endif()
                    # Must live inside the sysroot/toolchain tree (skipped only for a
                    # sysroot-less native build, where the host library is correct).
                    if (_math_roots)
                        set(_in_tree FALSE)
                        get_filename_component(_math_real "${_math_out}" REALPATH)
                        foreach (_root ${_math_roots})
                            get_filename_component(_root_real "${_root}" REALPATH)
                            string(FIND "${_math_real}" "${_root_real}/" _math_pos)
                            if (_math_pos EQUAL 0)
                                set(_in_tree TRUE)
                                break()
                            endif()
                        endforeach()
                        if (NOT _in_tree)
                            continue()
                        endif()
                    endif()
                    # Must define symbols (static archive: nm; shared object: nm -D);
                    # an empty stub is unusable.
                    execute_process(COMMAND "${NM_PATH}" "${_math_out}" OUTPUT_VARIABLE _nm_a ERROR_QUIET)
                    execute_process(COMMAND "${NM_PATH}" -D "${_math_out}" OUTPUT_VARIABLE _nm_d ERROR_QUIET)
                    if (NOT "${_nm_a}${_nm_d}" MATCHES "[0-9a-f] [TDBCWVi] ")
                        continue()
                    endif()
                    set(_found_math "${_math_out}")
                    break()
                endforeach()
                set(CH_RUST_LOCALIZE_MATHLIB "${_found_math}" CACHE INTERNAL
                    "Platform libm used as a reference for localizing Rust C symbols")
            endif()
            if (CH_RUST_LOCALIZE_MATHLIB)
                list(APPEND _localize_ref_libs "${CH_RUST_LOCALIZE_MATHLIB}")
            endif()
        endif()

        if (_localize_ref_libs)
            add_custom_target(localize_rust_c_${target_name}
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/localize_rust_c_symbols.sh"
                    "${_rust_lib_path}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${NM_PATH}"
                    ${_localize_ref_libs}
                COMMENT "Localizing C-namespace symbols in ${target_name}"
                VERBATIM
            )
            add_dependencies(localize_rust_c_${target_name}
                cargo-build_${target_name} ${_localize_ref_deps})
            add_dependencies(${target_name} localize_rust_c_${target_name})
        else()
            message(STATUS
                "localize_rust_c_symbols: no reference C libraries for this "
                "platform; skipping symbol localization for ${target_name}")
        endif()
    endif()
endfunction()
