if (OS_LINUX AND NOT SANITIZE AND NOT ARCH_ARM)
    set(ENABLE_JEMALLOC_DEFAULT 1)
else ()
    set(ENABLE_JEMALLOC_DEFAULT 0)
endif ()

option (ENABLE_JEMALLOC "Set to TRUE to use jemalloc" ${ENABLE_JEMALLOC_DEFAULT})
if (OS_LINUX)
    option (USE_INTERNAL_JEMALLOC_LIBRARY "Set to FALSE to use system jemalloc library instead of bundled" ${NOT_UNBUNDLED})
elseif ()
    option (USE_INTERNAL_JEMALLOC_LIBRARY "Set to FALSE to use system jemalloc library instead of bundled" OFF)
endif()

if (ENABLE_JEMALLOC)

    if (USE_INTERNAL_JEMALLOC_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/jemalloc/src/jemalloc.c")
       message (WARNING "submodule contrib/jemalloc is missing. to fix try run: \n git submodule update --init --recursive")
       set (USE_INTERNAL_JEMALLOC_LIBRARY 0)
       set (MISSING_INTERNAL_JEMALLOC_LIBRARY 1)
    endif ()

    if (NOT USE_INTERNAL_JEMALLOC_LIBRARY)
        find_package (JeMalloc)
    endif ()

    if ((NOT JEMALLOC_LIBRARIES OR NOT JEMALLOC_INCLUDE_DIR) AND NOT MISSING_INTERNAL_JEMALLOC_LIBRARY)
        set (JEMALLOC_LIBRARIES "jemalloc")
        set (JEMALLOC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/jemalloc/include")
        set (USE_INTERNAL_JEMALLOC_LIBRARY 1)
    endif ()

    if (JEMALLOC_LIBRARIES)
        set (USE_JEMALLOC 1)
    else ()
        message (FATAL_ERROR "ENABLE_JEMALLOC is set to true, but library was not found")
    endif ()

    if (SANITIZE)
        message (FATAL_ERROR "ENABLE_JEMALLOC is set to true, but it cannot be used with sanitizers")
    endif ()

    message (STATUS "Using jemalloc=${USE_JEMALLOC}: ${JEMALLOC_INCLUDE_DIR} : ${JEMALLOC_LIBRARIES}")
endif ()
