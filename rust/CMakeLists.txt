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

    set(RUST_VENDOR_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../contrib/rust_vendor")
endmacro()
configure_rustc()

# Add crate from the build directory.
# To avoid overlaps different builds for one source directory, crate will
# be copied from source directory to the binary directory.
function(add_rust_subdirectory src)
    set(dst "${CMAKE_CURRENT_BINARY_DIR}/${src}")
    message(STATUS "Copy ${src} to ${dst}")
    file(COPY "${src}" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}"
        PATTERN target EXCLUDE)

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

add_rust_subdirectory (workspace)
