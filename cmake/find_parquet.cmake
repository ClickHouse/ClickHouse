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
    include (CheckCXXSourceCompiles)
    # thrift uses deprecated feature. Remove this check if it fixed. https://github.com/apache/thrift/pull/1641
    cmake_push_check_state()
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z")
    check_cxx_source_compiles("
        #include <algorithm>
        #include <vector>
        using std::vector;
        int main() {
            vector<int> v;
            random_shuffle(v.begin(), v.end());
            return 0;
        }
        " HAVE_STD_RANDOM_SHUFFLE)
    cmake_pop_check_state()
    if(HAVE_STD_RANDOM_SHUFFLE)
        set(CAN_USE_INTERNAL_PARQUET_LIBRARY 1)
    endif()

   if(NOT CAN_USE_INTERNAL_PARQUET_LIBRARY)
        message(STATUS "Disabling internal parquet library because thrift is broken (can't use random_shuffle)")
        set(USE_INTERNAL_PARQUET_LIBRARY 0)
   else()
    set (USE_INTERNAL_PARQUET_LIBRARY 1)

    if (USE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE)
        set (ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")
        set (PARQUET_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src" ${ClickHouse_BINARY_DIR}/contrib/arrow/cpp/src)
    endif ()

    if (${USE_STATIC_LIBRARIES})
        set (ARROW_LIBRARY arrow_static)
        set (PARQUET_LIBRARY parquet_static)
        set (THRIFT_LIBRARY thrift_static)
    else ()
        set (ARROW_LIBRARY arrow_shared)
        set (PARQUET_LIBRARY parquet_shared)
        if (USE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE)
            list(APPEND PARQUET_LIBRARY ${Boost_REGEX_LIBRARY})
        endif ()
        set (THRIFT_LIBRARY thrift)
    endif ()

    set (USE_PARQUET 1)
   endif ()
endif ()

if (USE_PARQUET)
    message (STATUS "Using Parquet: ${ARROW_LIBRARY}:${ARROW_INCLUDE_DIR} ; ${PARQUET_LIBRARY}:${PARQUET_INCLUDE_DIR} ; ${THRIFT_LIBRARY}")
else ()
    message (STATUS "Building without Parquet support")
endif ()
