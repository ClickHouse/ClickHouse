option (USE_INTERNAL_GTEST_LIBRARY "Set to FALSE to use system Google Test instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_GTEST_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/googletest/googletets/CMakeLists.txt")
   message (WARNING "submodule contrib/googletest is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_GTEST_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_GTEST_LIBRARY)
    find_library (GTEST_LIBRARY gtest_main)
    find_path (GTEST_INCLUDE_DIR NAMES /gtest/gtest.h PATHS ${GTEST_INCLUDE_PATHS})
endif ()

if (GTEST_LIBRARY AND GTEST_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_GTEST_LIBRARY 1)
    set (GTEST_LIBRARY gtest_main)
endif ()

message (STATUS "Using gtest: ${GTEST_INCLUDE_DIR} : ${GTEST_LIBRARY}")
