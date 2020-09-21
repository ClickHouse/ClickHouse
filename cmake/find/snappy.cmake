option(USE_SNAPPY "Enable support of snappy library" ${ENABLE_LIBRARIES})

if(USE_SNAPPY)
    option (USE_INTERNAL_SNAPPY_LIBRARY "Set to FALSE to use system snappy library instead of bundled" ${NOT_UNBUNDLED})

    if(NOT USE_INTERNAL_SNAPPY_LIBRARY)
        find_library(SNAPPY_LIBRARY snappy)
    else ()
        set(SNAPPY_LIBRARY snappy)
    endif()

    message (STATUS "Using snappy: ${SNAPPY_LIBRARY}")
endif ()
