if (OS_LINUX AND NOT SANITIZE AND NOT ARCH_ARM AND NOT ARCH_32 AND NOT ARCH_PPC64LE)
    option (ENABLE_MIMALLOC "Set to FALSE to disable usage of mimalloc for internal ClickHouse caches" FALSE)
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/mimalloc/include/mimalloc.h")
    message (WARNING "submodule contrib/mimalloc is missing. to fix try run: \n git submodule update --init --recursive")
    return()
endif ()

if (ENABLE_MIMALLOC)
    message (FATAL_ERROR "Mimalloc is not production ready. (Disable with cmake -D ENABLE_MIMALLOC=0). If you want to use mimalloc, you must manually remove this message.")

    set (MIMALLOC_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/mimalloc/include)
    set (USE_MIMALLOC 1)
    set (MIMALLOC_LIBRARY mimalloc-static)
    message (STATUS "Using mimalloc: ${MIMALLOC_INCLUDE_DIR} : ${MIMALLOC_LIBRARY}")
endif ()
