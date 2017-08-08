option (ENABLE_JEMALLOC "Set to TRUE to use jemalloc instead of tcmalloc" OFF)

if (ENABLE_JEMALLOC)
    find_package (JeMalloc)

    if (JEMALLOC_INCLUDE_DIR AND JEMALLOC_LIBRARIES)
        set (USE_JEMALLOC 1)
        if (USE_TCMALLOC)
            message (WARNING "Disabling tcmalloc")
            set (USE_TCMALLOC 0)
        endif ()
    endif ()
    message (STATUS "Using jemalloc=${USE_JEMALLOC}: ${JEMALLOC_INCLUDE_DIR} : ${JEMALLOC_LIBRARIES}")
endif ()
