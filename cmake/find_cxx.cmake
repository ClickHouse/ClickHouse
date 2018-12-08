if (NOT APPLE)
    option (USE_INTERNAL_LIBCXX_LIBRARY "Set to FALSE to use system libcxx library instead of bundled" ${NOT_UNBUNDLED})
endif ()

if (USE_INTERNAL_LIBCXX_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libcxx/include/vector")
   message (WARNING "submodule contrib/libcxx is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_LIBCXX_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_LIBCXX_LIBRARY)
    find_library (LIBCXX_LIBRARY c++)
    find_path (LIBCXX_INCLUDE_DIR NAMES vector PATHS ${LIBCXX_INCLUDE_PATHS})
endif ()

if (LIBCXX_LIBRARY AND LIBCXX_INCLUDE_DIR)
else ()
    set (LIBCXX_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libcxx/include)
    set (USE_INTERNAL_LIBCXX_LIBRARY 1)
    set (LIBCXX_LIBRARY cxx)
endif ()

message (STATUS "Using libcxx: ${LIBCXX_INCLUDE_DIR} : ${LIBCXX_LIBRARY}")
