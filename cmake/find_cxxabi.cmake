if (NOT APPLE)
    option (USE_INTERNAL_LIBCXXABI_LIBRARY "Set to FALSE to use system libcxxabi library instead of bundled" ${NOT_UNBUNDLED})
endif ()

if (USE_INTERNAL_LIBCXXABI_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libcxxabi/src")
   message (WARNING "submodule contrib/libcxxabi is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_LIBCXXABI_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_LIBCXXABI_LIBRARY)
    find_library (LIBCXXABI_LIBRARY cxxabi)
    find_path (LIBCXXABI_INCLUDE_DIR NAMES vector PATHS ${LIBCXXABI_INCLUDE_PATHS})
endif ()

if (LIBCXXABI_LIBRARY AND LIBCXXABI_INCLUDE_DIR)
else ()
    set (LIBCXXABI_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libcxxabi/include)
    set (USE_INTERNAL_LIBCXXABI_LIBRARY 1)
    set (LIBCXXABI_LIBRARY cxxabi)
endif ()

message (STATUS "Using libcxxabi: ${LIBCXXABI_INCLUDE_DIR} : ${LIBCXXABI_LIBRARY}")
