# Link musl libc built from sources (contrib/musl-cmake) into everything.
# The CRT startup files are wired into every executable's link line in
# cmake/linux/default_libs.cmake; see the ordering comment there.
#
# Headers: the musl include directories (stubs for headers musl lacks, generated
# headers, arch-specific bits/*.h, generic arch fallbacks, public headers) are
# PUBLIC on the `musl` target and reach every target through `global-group`; the
# toolchain file additionally passes them for code compiled outside the normal
# target graph (configure-time compile checks). The toolchain also compiles
# everything with -nostdlibinc, so the glibc userspace headers from the sysroot
# are never searched: a header musl does not provide is a compile error, not a
# silent fallback to glibc declarations.

target_link_libraries(global-libs INTERFACE musl)

# The only part of the glibc sysroot that must stay reachable is the Linux kernel
# (uapi) headers: linux/, asm/, asm-generic/ and a few peripheral directories.
# Expose them through a directory of symlinks so that the sysroot's userspace
# headers remain unreachable. The `asm` directory lives in the multiarch
# subdirectory on some sysroots and at the top level on others. The toolchain
# file puts this directory on the include path of every compilation.
set (MUSL_KERNEL_HEADERS_DIR "${CMAKE_BINARY_DIR}/contrib/musl-cmake/kernel-headers")
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
