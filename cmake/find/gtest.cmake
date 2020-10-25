option (ENABLE_GTEST_LIBRARY "Enable gtest library" ${ENABLE_LIBRARIES})

if (ENABLE_GTEST_LIBRARY)

option (USE_INTERNAL_GTEST_LIBRARY "Set to FALSE to use system Google Test instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/googletest/googletest/CMakeLists.txt")
   if (USE_INTERNAL_GTEST_LIBRARY)
       message (WARNING "submodule contrib/googletest is missing. to fix try run: \n git submodule update --init --recursive")
       set (USE_INTERNAL_GTEST_LIBRARY 0)
   endif ()
   set (MISSING_INTERNAL_GTEST_LIBRARY 1)
endif ()


if(NOT USE_INTERNAL_GTEST_LIBRARY)
    # TODO: autodetect of GTEST_SRC_DIR by EXISTS /usr/src/googletest/CMakeLists.txt
    if(NOT GTEST_SRC_DIR)
        find_package(GTest)
    endif()
endif()

if (NOT GTEST_SRC_DIR AND NOT GTEST_INCLUDE_DIRS AND NOT MISSING_INTERNAL_GTEST_LIBRARY)
    set (USE_INTERNAL_GTEST_LIBRARY 1)
    set (GTEST_MAIN_LIBRARIES gtest_main)
    set (GTEST_LIBRARIES gtest)
    set (GTEST_BOTH_LIBRARIES ${GTEST_MAIN_LIBRARIES} ${GTEST_LIBRARIES})
    set (GTEST_INCLUDE_DIRS ${ClickHouse_SOURCE_DIR}/contrib/googletest/googletest)
endif ()

if((GTEST_INCLUDE_DIRS AND GTEST_BOTH_LIBRARIES) OR GTEST_SRC_DIR)
    set(USE_GTEST 1)
endif()

endif()

message (STATUS "Using gtest=${USE_GTEST}: ${GTEST_INCLUDE_DIRS} : ${GTEST_BOTH_LIBRARIES} : ${GTEST_SRC_DIR}")
