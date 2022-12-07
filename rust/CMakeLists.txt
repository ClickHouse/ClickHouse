function(configure_rustc)
    # NOTE: this can also be done by overriding rustc, but it not trivial with rustup.
    set(RUST_CFLAGS "${CMAKE_C_FLAGS}")

    set(CXX_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libcxx/include")
    set(RUST_CXXFLAGS "${CMAKE_CXX_FLAGS} -isystem ${CXX_INCLUDE_DIR} ")

    if (CMAKE_OSX_SYSROOT)
        set(RUST_CXXFLAGS "${RUST_CXXFLAGS} -isysroot ${CMAKE_OSX_SYSROOT}")
        set(RUST_CFLAGS "${RUST_CFLAGS} -isysroot ${CMAKE_OSX_SYSROOT}")
    elseif(CMAKE_SYSROOT)
        set(RUST_CXXFLAGS "${RUST_CXXFLAGS} --sysroot ${CMAKE_SYSROOT}")
        set(RUST_CFLAGS "${RUST_CFLAGS} --sysroot ${CMAKE_SYSROOT}")
    endif()

    message(STATUS "RUST_CFLAGS: ${RUST_CFLAGS}")
    message(STATUS "RUST_CXXFLAGS: ${RUST_CXXFLAGS}")

    # NOTE: requires RW access for the source dir
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml" @ONLY)
endfunction()
configure_rustc()

add_subdirectory (BLAKE3)
