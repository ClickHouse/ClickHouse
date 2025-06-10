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
set(Rust_TOOLCHAIN "nightly-2024-12-01")

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

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set(RUST_CFLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_DEBUG}")
    set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG} -isystem ${CXX_INCLUDE_DIR} -nostdinc++")
else ()
    set(RUST_CFLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_RELWITHDEBINFO}")
    set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -isystem ${CXX_INCLUDE_DIR} -nostdinc++")
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

if (USE_MUSL)
    set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -D_LIBCPP_HAS_MUSL_LIBC=1")
endif ()

if(CCACHE_EXECUTABLE MATCHES "/chcache$")
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
endif()

list(JOIN RUSTFLAGS " " RUSTFLAGS)

message(STATUS "RUST_CFLAGS: ${RUST_CFLAGS}")
message(STATUS "RUST_CXXFLAGS: ${RUST_CXXFLAGS}")
message(STATUS "RUSTFLAGS: ${RUSTFLAGS}")
message(STATUS "RUST_CARGO_BUILD_STD: ${RUST_CARGO_BUILD_STD}")

set(RUST_VENDOR_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../rust_vendor")

# We now prepare a custom config.toml with all the flags and add it to cargo via flags
configure_file("config.toml.in" "config.toml" @ONLY)
set (RUST_CONFIG_FILE "${CMAKE_CURRENT_BINARY_DIR}/config.toml" CACHE INTERNAL "Rust config")
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
    corrosion_link_libraries(${target_name} cxx)
endfunction()
