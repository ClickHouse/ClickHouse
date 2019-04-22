if (HAVE_SSSE3)
    option (ENABLE_HYPERSCAN "Enable hyperscan" ON)
endif ()

if (ENABLE_HYPERSCAN)

option (USE_INTERNAL_HYPERSCAN_LIBRARY "Set to FALSE to use system hyperscan instead of the bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/hyperscan/CMakeLists.txt")
    if (USE_INTERNAL_HYPERSCAN_LIBRARY)
        message (WARNING "submodule contrib/hyperscan is missing. to fix try run: \n git submodule update --init --recursive")
    endif ()
   set (MISSING_INTERNAL_HYPERSCAN_LIBRARY 1)
   set (USE_INTERNAL_HYPERSCAN_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_HYPERSCAN_LIBRARY)
    find_library (HYPERSCAN_LIBRARY hs)
    find_path (HYPERSCAN_INCLUDE_DIR NAMES hs/hs.h hs.h PATHS ${HYPERSCAN_INCLUDE_PATHS})
endif ()

if (HYPERSCAN_LIBRARY AND HYPERSCAN_INCLUDE_DIR)
    set (USE_HYPERSCAN 1)
elseif (NOT MISSING_INTERNAL_HYPERSCAN_LIBRARY)
    set (HYPERSCAN_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/hyperscan/src)
    set (HYPERSCAN_LIBRARY hs)
    set (USE_HYPERSCAN 1)
    set (USE_INTERNAL_HYPERSCAN_LIBRARY 1)
endif()

message (STATUS "Using hyperscan=${USE_HYPERSCAN}: ${HYPERSCAN_INCLUDE_DIR} : ${HYPERSCAN_LIBRARY}")

endif ()
