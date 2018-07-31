option (ENABLE_JEMALLOC "Set to TRUE to use jemalloc" ON)
option (USE_INTERNAL_JEMALLOC_LIBRARY "Set to FALSE to use system jemalloc library instead of bundled" ${NOT_UNBUNDLED})

if (ENABLE_JEMALLOC)
    if (USE_INTERNAL_JEMALLOC_LIBRARY)
        set (JEMALLOC_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/jemalloc/include" "${ClickHouse_BINARY_DIR}/contrib/jemalloc/include")
        set (JEMALLOC_LIBRARIES "jemalloc")
    else ()
        find_package (JeMalloc)
    endif ()

    if (JEMALLOC_INCLUDE_DIR AND JEMALLOC_LIBRARIES)
        set (USE_JEMALLOC 1)
    else ()
        message (FATAL_ERROR "ENABLE_JEMALLOC is set to true, but library was not found")
    endif ()

    message (STATUS "Using jemalloc=${USE_JEMALLOC}: ${JEMALLOC_INCLUDE_DIR} : ${JEMALLOC_LIBRARIES}")
endif ()
