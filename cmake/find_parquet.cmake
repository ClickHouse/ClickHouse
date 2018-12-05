if (NOT OS_FREEBSD) # Freebsd: ../contrib/arrow/cpp/src/arrow/util/bit-util.h:27:10: fatal error: endian.h: No such file or directory
    option (USE_INTERNAL_PARQUET_LIBRARY "Set to FALSE to use system parquet library instead of bundled" ${NOT_UNBUNDLED})
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/CMakeLists.txt")
    if (USE_INTERNAL_PARQUET_LIBRARY)
        message (WARNING "submodule contrib/arrow (required for Parquet) is missing. to fix try run: \n git submodule update --init --recursive")
    endif ()
    set (USE_INTERNAL_PARQUET_LIBRARY 0)
    set (MISSING_INTERNAL_PARQUET_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_PARQUET_LIBRARY)
    find_package (Arrow)
    find_package (Parquet)
endif ()

if (ARROW_INCLUDE_DIR AND PARQUET_INCLUDE_DIR)
elseif (NOT MISSING_INTERNAL_PARQUET_LIBRARY AND NOT OS_FREEBSD)
    set (USE_INTERNAL_PARQUET_LIBRARY 1)
    # TODO: is it required?
    # set (ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/arrow")
    # set (PARQUET_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src/parquet")
    if (${USE_STATIC_LIBRARIES})
        set (ARROW_LIBRARY arrow_static)
        set (PARQUET_LIBRARY parquet_static)
    else ()
        set (ARROW_LIBRARY arrow_shared)
        set (PARQUET_LIBRARY parquet_shared)
    endif ()

    set (USE_PARQUET 1)
endif ()

if (USE_PARQUET)
    message (STATUS "Using Parquet: ${ARROW_LIBRARY}:${ARROW_INCLUDE_DIR} ; ${PARQUET_LIBRARY}:${PARQUET_INCLUDE_DIR}")
else ()
    message (STATUS "Building without Parquet support")
endif ()
