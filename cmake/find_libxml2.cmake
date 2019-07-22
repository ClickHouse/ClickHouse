option (USE_INTERNAL_LIBXML2_LIBRARY "Set to FALSE to use system libxml2 library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libxml2/libxml.h")
    if (USE_INTERNAL_LIBXML2_LIBRARY)
        message (WARNING "submodule contrib/libxml2 is missing. to fix try run: \n git submodule update --init --recursive")
        set (USE_INTERNAL_LIBXML2_LIBRARY 0)
    endif ()
    set (MISSING_INTERNAL_LIBXML2_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_LIBXML2_LIBRARY)
    find_package (LibXml2)
    #find_library (LIBXML2_LIBRARY libxml2)
    #find_path (LIBXML2_INCLUDE_DIR NAMES libxml.h PATHS ${LIBXML2_INCLUDE_PATHS})
endif ()

if (LIBXML2_LIBRARY AND LIBXML2_INCLUDE_DIR)
elseif (NOT MISSING_INTERNAL_LIBXML2_LIBRARY)
    set (LIBXML2_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libxml2/include ${ClickHouse_SOURCE_DIR}/contrib/libxml2-cmake/linux_x86_64/include)
    set (USE_INTERNAL_LIBXML2_LIBRARY 1)
    set (LIBXML2_LIBRARY libxml2)
endif ()

message (STATUS "Using libxml2: ${LIBXML2_INCLUDE_DIR} : ${LIBXML2_LIBRARY}")
