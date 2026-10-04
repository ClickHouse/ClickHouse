# getcontext/setcontext/swapcontext/makecontext for musl, which deliberately
# does not implement them. Needed by boost::context's ucontext backend, which
# is used in sanitizer builds (see contrib/boost-cmake).

if (NOT USE_MUSL)
    message(STATUS "Not using libucontext")
    return()
endif()

set(LIBUCONTEXT_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libucontext")

set(SRCS
    "${LIBUCONTEXT_SOURCE_DIR}/arch/${MUSL_ARCH}/getcontext.S"
    "${LIBUCONTEXT_SOURCE_DIR}/arch/${MUSL_ARCH}/setcontext.S"
    "${LIBUCONTEXT_SOURCE_DIR}/arch/${MUSL_ARCH}/swapcontext.S"
    "${LIBUCONTEXT_SOURCE_DIR}/arch/${MUSL_ARCH}/makecontext.c"
    "${LIBUCONTEXT_SOURCE_DIR}/arch/${MUSL_ARCH}/trampoline.c"
)

add_library(_ucontext ${SRCS})

# EXPORT_UNPREFIXED: provide the standard names as weak aliases of libucontext_*.
# _BSD_SOURCE: musl's bits/signal.h exposes greg_t/mcontext_t details under it,
# without the REG_* enums of _GNU_SOURCE that clash with libucontext's defs.h.
target_compile_definitions(_ucontext PRIVATE EXPORT_UNPREFIXED _BSD_SOURCE)
target_include_directories(_ucontext PRIVATE
    "${LIBUCONTEXT_SOURCE_DIR}/include"
    "${LIBUCONTEXT_SOURCE_DIR}/arch/common/include"
    "${LIBUCONTEXT_SOURCE_DIR}/arch/common"
)

add_library(ch_contrib::ucontext ALIAS _ucontext)
