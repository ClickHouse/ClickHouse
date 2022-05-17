if (NOT SANITIZE)
    message("Not sanitize, so will use mimalloc")
    option (ENABLE_MIMALLOC "Enable mimalloc allocator" ON)
else()
    message("Not using mimalloc")
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/mimalloc/include/mimalloc.h")
   message (WARNING "submodule contrib/mimalloc is missing. to fix try run: \n git submodule update --init --recursive")
endif ()

set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/mimalloc)

set (SRCS
    ${LIBRARY_DIR}/src/stats.c
    ${LIBRARY_DIR}/src/random.c
    ${LIBRARY_DIR}/src/os.c
    ${LIBRARY_DIR}/src/bitmap.c
    ${LIBRARY_DIR}/src/arena.c
    ${LIBRARY_DIR}/src/segment-cache.c
    ${LIBRARY_DIR}/src/segment.c
    ${LIBRARY_DIR}/src/page.c
    ${LIBRARY_DIR}/src/alloc.c
    ${LIBRARY_DIR}/src/alloc-aligned.c
    ${LIBRARY_DIR}/src/alloc-posix.c
    ${LIBRARY_DIR}/src/heap.c
    ${LIBRARY_DIR}/src/options.c
    ${LIBRARY_DIR}/src/init.c
)

if (ENABLE_MIMALLOC)
    set (MIMALLOC_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/mimalloc/include)
    set (USE_MIMALLOC 1)

    add_library(_mimalloc ${SRCS})
    target_compile_options(_mimalloc PRIVATE
        # Note: turn of this warning:
        #   error: identifier '_mi_option_last' is reserved because it starts with '_' at global scope
        -Wno-reserved-identifier
    )
    target_compile_definitions(_mimalloc PRIVATE -DMI_MALLOC_OVERRIDE)
    target_include_directories(_mimalloc SYSTEM BEFORE PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/mimalloc/include")
    add_library(ch_contrib::mimalloc ALIAS _mimalloc)
    message("Added mimalloc library")
endif ()
