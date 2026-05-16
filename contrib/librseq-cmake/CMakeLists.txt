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

add_library (ch_contrib::librseq ALIAS _librseq)
