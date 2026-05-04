option (ENABLE_LIBRSEQ "Enable librseq" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LIBRSEQ)
    message (STATUS "Not using librseq")
    return ()
endif ()

if (NOT OS_LINUX OR NOT (ARCH_AMD64 OR ARCH_AARCH64))
    message (STATUS "Not using librseq: only Linux x86_64 / aarch64 are supported")
    return ()
endif ()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/librseq")

# Skip rseq-mempool.c (per-CPU memory pool, depends on optional libnuma) —
# we only need the registration / RMW core. rseq-status.c is a CLI tool.
set (SRCS
    "${LIBRARY_DIR}/src/rseq.c"
    "${LIBRARY_DIR}/src/smp.c"
)

add_library (_librseq ${SRCS})

target_include_directories (_librseq SYSTEM PUBLIC "${LIBRARY_DIR}/include")

# librseq sources require _GNU_SOURCE for getauxval / sched_getcpu /
# struct rseq layout; smp.c marks helpers _LGPL_SOURCE-only via the LGPL header.
target_compile_definitions (_librseq PRIVATE _GNU_SOURCE)

# Suppress upstream warnings — we do not own the code, and our project
# enables -Werror for first-party sources only.
target_compile_options (_librseq PRIVATE
    -Wno-pedantic
    -Wno-reserved-identifier
    -Wno-reserved-macro-identifier
    -Wno-unused-parameter
    -Wno-unused-variable
    -Wno-sign-compare
    -Wno-cast-qual
    -Wno-old-style-cast
    -Wno-extra-semi-stmt
    -Wno-zero-length-array
    -Wno-gnu-zero-variadic-macro-arguments
    -Wno-documentation
)

add_library (ch_contrib::librseq ALIAS _librseq)
