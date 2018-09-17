set(JEMALLOC_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/jemalloc)

set(SRCS
${JEMALLOC_SOURCE_DIR}/src/arena.c
${JEMALLOC_SOURCE_DIR}/src/background_thread.c
${JEMALLOC_SOURCE_DIR}/src/base.c
${JEMALLOC_SOURCE_DIR}/src/bin.c
${JEMALLOC_SOURCE_DIR}/src/bitmap.c
${JEMALLOC_SOURCE_DIR}/src/ckh.c
${JEMALLOC_SOURCE_DIR}/src/ctl.c
${JEMALLOC_SOURCE_DIR}/src/div.c
${JEMALLOC_SOURCE_DIR}/src/extent.c
${JEMALLOC_SOURCE_DIR}/src/extent_dss.c
${JEMALLOC_SOURCE_DIR}/src/extent_mmap.c
${JEMALLOC_SOURCE_DIR}/src/hash.c
${JEMALLOC_SOURCE_DIR}/src/hook.c
${JEMALLOC_SOURCE_DIR}/src/jemalloc.c
${JEMALLOC_SOURCE_DIR}/src/jemalloc_cpp.cpp
${JEMALLOC_SOURCE_DIR}/src/large.c
${JEMALLOC_SOURCE_DIR}/src/log.c
${JEMALLOC_SOURCE_DIR}/src/malloc_io.c
${JEMALLOC_SOURCE_DIR}/src/mutex.c
${JEMALLOC_SOURCE_DIR}/src/mutex_pool.c
${JEMALLOC_SOURCE_DIR}/src/nstime.c
${JEMALLOC_SOURCE_DIR}/src/pages.c
${JEMALLOC_SOURCE_DIR}/src/prng.c
${JEMALLOC_SOURCE_DIR}/src/prof.c
${JEMALLOC_SOURCE_DIR}/src/rtree.c
${JEMALLOC_SOURCE_DIR}/src/sc.c
${JEMALLOC_SOURCE_DIR}/src/stats.c
${JEMALLOC_SOURCE_DIR}/src/sz.c
${JEMALLOC_SOURCE_DIR}/src/tcache.c
${JEMALLOC_SOURCE_DIR}/src/test_hooks.c
${JEMALLOC_SOURCE_DIR}/src/ticker.c
${JEMALLOC_SOURCE_DIR}/src/tsd.c
${JEMALLOC_SOURCE_DIR}/src/witness.c
)

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  list(APPEND SRCS ${JEMALLOC_SOURCE_DIR}/src/zone.c)
endif()

add_library(jemalloc STATIC ${SRCS})

target_include_directories(jemalloc PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include_linux_x86_64) # jemalloc.h

target_include_directories(jemalloc PRIVATE
    ${JEMALLOC_SOURCE_DIR}/include)

target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_DEBUG=1)
endif ()
