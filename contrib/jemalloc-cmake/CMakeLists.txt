option (ENABLE_JEMALLOC "Enable jemalloc allocator" ${ENABLE_LIBRARIES})

if (SANITIZE OR NOT OS_LINUX OR NOT (ARCH_AMD64 OR ARCH_ARM))
    set (ENABLE_JEMALLOC OFF)
    message (STATUS "jemalloc is disabled implicitly: it doesn't work with sanitizers and can only be used on Linux with x86_64 or aarch64.")
endif ()

if (ENABLE_JEMALLOC)
    option (USE_INTERNAL_JEMALLOC "Use internal jemalloc library" ${NOT_UNBUNDLED})

    if (USE_INTERNAL_JEMALLOC)
        set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/jemalloc")

        set (SRCS
            ${LIBRARY_DIR}/src/arena.c
            ${LIBRARY_DIR}/src/background_thread.c
            ${LIBRARY_DIR}/src/base.c
            ${LIBRARY_DIR}/src/bin.c
            ${LIBRARY_DIR}/src/bitmap.c
            ${LIBRARY_DIR}/src/ckh.c
            ${LIBRARY_DIR}/src/ctl.c
            ${LIBRARY_DIR}/src/div.c
            ${LIBRARY_DIR}/src/extent.c
            ${LIBRARY_DIR}/src/extent_dss.c
            ${LIBRARY_DIR}/src/extent_mmap.c
            ${LIBRARY_DIR}/src/hash.c
            ${LIBRARY_DIR}/src/hook.c
            ${LIBRARY_DIR}/src/jemalloc.c
            ${LIBRARY_DIR}/src/large.c
            ${LIBRARY_DIR}/src/log.c
            ${LIBRARY_DIR}/src/malloc_io.c
            ${LIBRARY_DIR}/src/mutex.c
            ${LIBRARY_DIR}/src/mutex_pool.c
            ${LIBRARY_DIR}/src/nstime.c
            ${LIBRARY_DIR}/src/pages.c
            ${LIBRARY_DIR}/src/prng.c
            ${LIBRARY_DIR}/src/prof.c
            ${LIBRARY_DIR}/src/rtree.c
            ${LIBRARY_DIR}/src/sc.c
            ${LIBRARY_DIR}/src/stats.c
            ${LIBRARY_DIR}/src/sz.c
            ${LIBRARY_DIR}/src/tcache.c
            ${LIBRARY_DIR}/src/test_hooks.c
            ${LIBRARY_DIR}/src/ticker.c
            ${LIBRARY_DIR}/src/tsd.c
            ${LIBRARY_DIR}/src/witness.c
        )
        if (OS_DARWIN)
            list(APPEND SRCS ${LIBRARY_DIR}/src/zone.c)
        endif ()

        add_library(jemalloc ${SRCS})
        target_include_directories(jemalloc PRIVATE ${LIBRARY_DIR}/include)
        target_include_directories(jemalloc PUBLIC include)
        if (ARCH_AMD64)
            target_include_directories(jemalloc PUBLIC include_linux_x86_64)
        elseif (ARCH_ARM)
            target_include_directories(jemalloc PUBLIC include_linux_aarch64)
        endif ()

        target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)

        if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
            target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_DEBUG=1 -DJEMALLOC_PROF=1)

            if (USE_UNWIND)
                target_compile_definitions (jemalloc PRIVATE -DJEMALLOC_PROF_LIBUNWIND=1)
                target_link_libraries (jemalloc PRIVATE unwind)
            endif ()
        endif ()

        target_compile_options(jemalloc PRIVATE -Wno-redundant-decls)
    else ()
        find_library(LIBRARY_JEMALLOC jemalloc)
        find_path(INCLUDE_JEMALLOC jemalloc/jemalloc.h)

        set(THREADS_PREFER_PTHREAD_FLAG ON)
        find_package(Threads REQUIRED)

        add_library (jemalloc STATIC IMPORTED)
        set_property (TARGET jemalloc PROPERTY IMPORTED_LOCATION ${LIBRARY_JEMALLOC})
        set_property (TARGET jemalloc PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_JEMALLOC})
        set_property (TARGET jemalloc PROPERTY INTERFACE_LINK_LIBRARIES Threads::Threads dl)

        set (CMAKE_REQUIRED_LIBRARIES jemalloc)
        check_cxx_source_compiles (
            "
            #include <jemalloc/jemalloc.h>

            int main() {
                free(mallocx(1, 0));
            }
            "
            EXTERNAL_JEMALLOC_WORKS
        )

        if (NOT EXTERNAL_JEMALLOC_WORKS)
            message (FATAL_ERROR "jemalloc is unusable: ${LIBRARY_JEMALLOC} ${INCLUDE_JEMALLOC}")
        endif ()
    endif ()

    set_property(TARGET jemalloc APPEND PROPERTY INTERFACE_COMPILE_DEFINITIONS USE_JEMALLOC=1)

    message (STATUS "Using jemalloc")
else ()
    add_library(jemalloc INTERFACE)
    target_compile_definitions(jemalloc INTERFACE USE_JEMALLOC=0)

    message (STATUS "Not using jemalloc")
endif ()
