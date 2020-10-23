set(JEMALLOC_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/jemalloc)

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

if(OS_DARWIN)
  list(APPEND SRCS ${JEMALLOC_SOURCE_DIR}/src/zone.c)
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
endif ()

add_library(jemalloc STATIC ${SRCS})

target_include_directories(jemalloc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

if (ARCH_AMD64)
    target_include_directories(jemalloc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include_linux_x86_64)
elseif (ARCH_ARM)
    target_include_directories(jemalloc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include_linux_aarch64)
else ()
    message (FATAL_ERROR "jemalloc can only be used on x86_64 or aarch64.")
endif ()

target_include_directories(jemalloc PRIVATE
    ${JEMALLOC_SOURCE_DIR}/include)

target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_DEBUG=1 -DJEMALLOC_PROF=1)

    if (USE_UNWIND)
        target_compile_definitions (jemalloc PRIVATE -DJEMALLOC_PROF_LIBUNWIND=1)
        target_link_libraries (jemalloc PRIVATE ${UNWIND_LIBRARIES})
    endif ()
endif ()
