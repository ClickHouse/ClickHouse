
option (ENABLE_VECTORCLASS "Faster math functions with vectorclass lib" OFF)

if (ENABLE_VECTORCLASS)

    set (VECTORCLASS_INCLUDE_PATHS "${ClickHouse_SOURCE_DIR}/contrib/vectorclass" CACHE STRING "Path of vectorclass library")
    find_path (VECTORCLASS_INCLUDE_DIR NAMES vectorf128.h PATHS ${VECTORCLASS_INCLUDE_PATHS})

    if (VECTORCLASS_INCLUDE_DIR)
        set (USE_VECTORCLASS 1)
    endif ()

    message (STATUS "Using vectorclass=${USE_VECTORCLASS}: ${VECTORCLASS_INCLUDE_DIR}")

endif ()
