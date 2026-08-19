# Link musl libc built from sources (contrib/musl-cmake) into everything.
# The CRT objects cannot be linked here: CMake's Ninja generator does not support
# $<TARGET_OBJECTS:...> in INTERFACE_LINK_LIBRARIES, so clickhouse_add_executable
# adds them to each executable instead.

target_link_libraries(global-libs INTERFACE ${MUSL_LIBC_TARGET})

# musl headers, in priority order: stubs for headers musl lacks (execinfo.h),
# generated headers, arch-specific bits/*.h, generic arch fallbacks, public headers.
target_include_directories(global-libs SYSTEM BEFORE INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/musl-cmake/include"
    "${ClickHouse_BINARY_DIR}/contrib/musl-cmake/include"
    "${ClickHouse_SOURCE_DIR}/contrib/musl/arch/${MUSL_ARCH}"
    "${ClickHouse_SOURCE_DIR}/contrib/musl/arch/generic"
    "${ClickHouse_SOURCE_DIR}/contrib/musl/include"
)

# The only part of the glibc sysroot that must stay reachable is the Linux kernel
# (uapi) headers: linux/, asm/, asm-generic/ and a few peripheral directories.
# Expose them through a directory of symlinks so that the sysroot's userspace
# headers remain unreachable (the toolchain files compile everything with
# -nostdlibinc): a header musl does not provide is a compile error, not a silent
# fallback to glibc declarations. The `asm` directory lives in the multiarch
# subdirectory on some sysroots and at the top level on others.
set (MUSL_KERNEL_HEADERS_DIR "${ClickHouse_BINARY_DIR}/contrib/musl-cmake/kernel-headers")
file (MAKE_DIRECTORY "${MUSL_KERNEL_HEADERS_DIR}")
foreach (dir asm asm-generic linux drm misc mtd rdma scsi sound video xen)
    set (link "${MUSL_KERNEL_HEADERS_DIR}/${dir}")
    if (IS_SYMLINK "${link}" AND NOT EXISTS "${link}")
        file (REMOVE "${link}")
    endif ()
    foreach (base "${CMAKE_SYSROOT}/usr/include/${MUSL_ARCH}-linux-gnu" "${CMAKE_SYSROOT}/usr/include")
        if (EXISTS "${base}/${dir}" AND NOT EXISTS "${link}")
            file (CREATE_LINK "${base}/${dir}" "${link}" SYMBOLIC)
        endif ()
    endforeach ()
endforeach ()

target_include_directories(global-libs SYSTEM INTERFACE "${MUSL_KERNEL_HEADERS_DIR}")
