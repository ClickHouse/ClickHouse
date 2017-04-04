option (USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY "Set to FALSE to use system double-conversion library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY)
    find_library (DOUBLE_CONVERSION_LIBRARY double-conversion)
    find_path (DOUBLE_CONVERSION_INCLUDE_DIR NAMES double-conversion/double-conversion.h PATHS ${DOUBLE_CONVERSION_INCLUDE_PATHS})
endif ()

if (DOUBLE_CONVERSION_LIBRARY AND DOUBLE_CONVERSION_INCLUDE_DIR)
    include_directories (${DOUBLE_CONVERSION_INCLUDE_DIR})
else ()
    set (USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY 1)
    set (DOUBLE_CONVERSION_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libdouble-conversion")
    include_directories (BEFORE ${DOUBLE_CONVERSION_INCLUDE_DIR})
    set (DOUBLE_CONVERSION_LIBRARY double-conversion)
endif ()

message (STATUS "Using double-conversion: ${DOUBLE_CONVERSION_INCLUDE_DIR} : ${DOUBLE_CONVERSION_LIBRARY}")
