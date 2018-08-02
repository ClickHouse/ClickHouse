# bundled jemalloc only for linux x64
if (NOT ARCH_FREEBSD AND NOT APPLE)
    option (ENABLE_JEMALLOC "Set to TRUE to use jemalloc" ON)
    option (USE_INTERNAL_JEMALLOC_LIBRARY "Set to FALSE to use system jemalloc library instead of bundled" ${NOT_UNBUNDLED})
endif ()

if (ENABLE_JEMALLOC)
    if (NOT USE_INTERNAL_JEMALLOC_LIBRARY)
        find_package (JeMalloc)
    endif ()

    if (NOT JEMALLOC_LIBRARIES)
        set (JEMALLOC_LIBRARIES "jemalloc")
        set (USE_INTERNAL_JEMALLOC_LIBRARY 1)
    endif ()

    if (JEMALLOC_LIBRARIES)
        set (USE_JEMALLOC 1)
    else ()
        message (FATAL_ERROR "ENABLE_JEMALLOC is set to true, but library was not found")
    endif ()

    message (STATUS "Using jemalloc=${USE_JEMALLOC}: ${JEMALLOC_LIBRARIES}")
endif ()
