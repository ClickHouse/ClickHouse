# Cross-compile from Linux to illumos using LLD. Native illumos builds do not load this file.
# See linux/toolchain-x86_64.cmake for details about loading a toolchain twice.
# Put `ld.lld` on PATH, or set `ILLUMOS_LLD` to its path.
include_guard(GLOBAL)

set (CMAKE_SYSTEM_NAME "SunOS")
set (CMAKE_SYSTEM_PROCESSOR "x86_64")
set (CMAKE_C_COMPILER_TARGET "x86_64-pc-solaris2.11")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-pc-solaris2.11")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-pc-solaris2.11")
set (CMAKE_SYSROOT "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/illumos-x86_64")
set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

# For this cross-build, Clang's Solaris driver still passes the native linker's
# -C option, which LLD rejects. Remove it in a wrapper.
list (APPEND CMAKE_TRY_COMPILE_PLATFORM_VARIABLES ILLUMOS_LLD)
find_program (ILLUMOS_LLD NAMES ld.lld REQUIRED)
configure_file ("${CMAKE_CURRENT_LIST_DIR}/linker.in" "${CMAKE_BINARY_DIR}/illumos-ld" @ONLY)
file (CHMOD "${CMAKE_BINARY_DIR}/illumos-ld" PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

foreach (lang C CXX ASM)
    set (CMAKE_${lang}_FLAGS "${CMAKE_${lang}_FLAGS} --no-default-config")
endforeach ()

# Clang's Solaris driver does not request --eh-frame-hdr when using LLD.
# Add it for this cross-build so the bundled libunwind can find unwind information.
foreach (kind EXE MODULE SHARED)
    set (CMAKE_${kind}_LINKER_FLAGS "${CMAKE_${kind}_LINKER_FLAGS} --no-default-config -fuse-ld=${CMAKE_BINARY_DIR}/illumos-ld -Wl,--eh-frame-hdr,--dynamic-linker=/lib/amd64/ld.so.1,--sysroot=${CMAKE_SYSROOT},-rpath-link,${CMAKE_SYSROOT}/lib/amd64,-rpath-link,${CMAKE_SYSROOT}/usr/lib/amd64")
endforeach ()
