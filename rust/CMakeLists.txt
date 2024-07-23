# NOTE: should be macro to export RUST_CXXFLAGS/RUST_CFLAGS for subfolders
macro(configure_rustc)
    # NOTE: this can also be done by overriding rustc, but it not trivial with rustup.
    set(RUST_CFLAGS "${CMAKE_C_FLAGS}")

    set(CXX_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libcxx/include")
    set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} -isystem ${CXX_INCLUDE_DIR} -nostdinc++")

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

    if(CCACHE_EXECUTABLE MATCHES "/sccache$")
        message(STATUS "Using RUSTC_WRAPPER: ${CCACHE_EXECUTABLE}")
        set(RUSTCWRAPPER "rustc-wrapper = \"${CCACHE_EXECUTABLE}\"")
    else()
        set(RUSTCWRAPPER "")
    endif()

    set(RUSTFLAGS)
    if (CMAKE_OSX_DEPLOYMENT_TARGET)
        list(APPEND RUSTFLAGS "-C" "link-arg=-mmacosx-version-min=${CMAKE_OSX_DEPLOYMENT_TARGET}")
    endif()

    set(RUST_CARGO_BUILD_STD "")
    # For more info: https://doc.rust-lang.org/beta/unstable-book/compiler-flags/sanitizer.html#memorysanitizer
    if (SANITIZE STREQUAL "memory")
        set(RUST_CARGO_BUILD_STD "build-std = [\"std\", \"panic_abort\", \"core\", \"alloc\"]")
        list(APPEND RUSTFLAGS "-Zsanitizer=memory" "-Zsanitizer-memory-track-origins")
    endif()

    list(TRANSFORM RUSTFLAGS PREPEND "\"")
    list(TRANSFORM RUSTFLAGS APPEND "\"")
    list(JOIN RUSTFLAGS "," RUSTFLAGS)
    set(RUSTFLAGS "[${RUSTFLAGS}]")

    message(STATUS "RUST_CFLAGS: ${RUST_CFLAGS}")
    message(STATUS "RUST_CXXFLAGS: ${RUST_CXXFLAGS}")
    message(STATUS "RUSTFLAGS: ${RUSTFLAGS}")
    message(STATUS "RUST_CARGO_BUILD_STD: ${RUST_CARGO_BUILD_STD}")

    # NOTE: requires RW access for the source dir
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml" @ONLY)
endmacro()
configure_rustc()

function(clickhouse_import_crate)
    # This is a workaround for Corrosion case sensitive build type matching in
    # _generator_add_cargo_targets(), that leads to different paths in
    # IMPORTED_LOCATION and real path of the library.
    #
    # It uses CMAKE_CONFIGURATION_TYPES and $<CONFIG>, so here we preserve the
    # case of ${CMAKE_BUILD_TYPE} in ${CMAKE_CONFIGURATION_TYPES}.
    if ("${CMAKE_BUILD_TYPE_UC}" STREQUAL "DEBUG")
        set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE};release")
    else()
        set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE};debug")
    endif()

    if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
        set(profile "")
    else()
        if (ENABLE_THINLTO)
            set(profile "release-thinlto")
        else()
            set(profile "release")
        endif()
    endif()

    # Note, here --offline is not used, since on CI vendor archive is used, and
    # passing --offline here will be inconvenient for local development.
    corrosion_import_crate(NO_STD ${ARGN} PROFILE ${profile})
endfunction()

# Add crate from the build directory.
#
# Our crates has configuration files:
# - config for cargo (see config.toml.in)
# - and possibly config for build (build.rs.in)
#
# And to avoid overlaps different builds for one source directory, crate will
# be copied from source directory to the binary directory.
file(COPY ".cargo" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}")
function(add_rust_subdirectory src)
    set(dst "${CMAKE_CURRENT_BINARY_DIR}/${src}")
    message(STATUS "Copy ${src} to ${dst}")
    file(COPY "${src}" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}"
        PATTERN target EXCLUDE)

    # Check is Rust available or not.
    #
    # `cargo update --dry-run` will not update anything, but will check the internet connectivity.
    execute_process(COMMAND ${Rust_CARGO_CACHED} update --dry-run
        WORKING_DIRECTORY "${dst}"
        RESULT_VARIABLE CARGO_UPDATE_RESULT
        OUTPUT_VARIABLE CARGO_UPDATE_STDOUT
        ERROR_VARIABLE CARGO_UPDATE_STDERR)
    if (CARGO_UPDATE_RESULT)
        message(FATAL_ERROR "Rust (${Rust_CARGO_CACHED}) support is not available (likely there is no internet connectivity):\n${CARGO_UPDATE_STDERR}\nYou can disable Rust support with -DENABLE_RUST=OFF")
    endif()

    add_subdirectory("${dst}" "${dst}")

    # cmake -E copy* do now know how to exclude files
    # but we need to exclude "target" folder from copying, if someone or semantic
    # completion created it.
    add_custom_target(${src}-update ALL
        COMMAND ${CMAKE_COMMAND}
            -DFROM=${src}
            -DTO=${CMAKE_CURRENT_BINARY_DIR}
            -P "${CMAKE_CURRENT_SOURCE_DIR}/copy_exclude.cmake"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        VERBATIM)
endfunction()

add_rust_subdirectory (skim)
add_rust_subdirectory (prql)
