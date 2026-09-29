if (NOT OS_LINUX)
    message (STATUS "Not building librseq because it is Linux-only (uses rseq syscall and <linux/*> headers)")
    return ()
endif ()

set (LIBRSEQ_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/librseq")

add_library (_librseq STATIC
    "${LIBRSEQ_SOURCE_DIR}/src/rseq.c"
    "${LIBRSEQ_SOURCE_DIR}/src/smp.c"
)

target_include_directories (_librseq SYSTEM
    PUBLIC  "${LIBRSEQ_SOURCE_DIR}/include"
    PRIVATE "${LIBRSEQ_SOURCE_DIR}/src"
)

target_compile_definitions (_librseq PRIVATE _GNU_SOURCE)

if (USE_MUSL)
    # librseq headers use glibc's __GNUC_PREREQ from <features.h>, which musl does not
    # provide. CMake drops function-style macros from COMPILE_DEFINITIONS, so force-include
    # a shim header instead. PRIVATE because sanitize_targets.cmake forbids interface
    # compile options on contrib targets; users of librseq headers (silk) force-include
    # it themselves in their contrib CMake.
    target_compile_options (_librseq PRIVATE "SHELL:-include ${CMAKE_CURRENT_SOURCE_DIR}/musl-compat.h")

    # In our static musl binaries, dlsym cannot resolve the libc rseq ABI symbols that
    # rseq_init looks up; redirect the lookup to weak link-time references (see the
    # header for details and the upstream plan).
    target_compile_options (_librseq PRIVATE "SHELL:-include ${CMAKE_CURRENT_SOURCE_DIR}/static-dlsym-compat.h")
endif ()

add_library (ch_contrib::librseq ALIAS _librseq)
