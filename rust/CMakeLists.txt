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
add_rust_subdirectory (chcache)
