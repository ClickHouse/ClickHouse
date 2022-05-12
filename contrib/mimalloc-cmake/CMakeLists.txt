if (OS_LINUX AND NOT SANITIZE AND NOT ARCH_ARM AND NOT ARCH_32 AND NOT ARCH_PPC64LE)
    option (ENABLE_MIMALLOC "Enable mimalloc allocator" ${ENABLE_LIBRARIES})
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/mimalloc/include/mimalloc.h")
   message (WARNING "submodule contrib/mimalloc is missing. to fix try run: \n git submodule update --init --recursive")
endif ()

if (ENABLE_MIMALLOC)
    set (MIMALLOC_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/mimalloc/include)
    set (USE_MIMALLOC 1)
    set (MIMALLOC_LIBRARY mimalloc-static)
    message (STATUS "Using mimalloc: ${MIMALLOC_INCLUDE_DIR} : ${MIMALLOC_LIBRARY}")

    add_library(_mimalloc INTERFACE)
    target_include_directories(_msgpack SYSTEM BEFORE INTERFACE ${MIMALLOC_INCLUDE_DIR})
    add_library(ch_contrib::mimalloc ALIAS _mimalloc)
endif ()
