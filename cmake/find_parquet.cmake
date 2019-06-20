option (ENABLE_PARQUET "Enable parquet" ON)

if (ENABLE_PARQUET)

if (NOT OS_FREEBSD) # Freebsd: ../contrib/arrow/cpp/src/arrow/util/bit-util.h:27:10: fatal error: endian.h: No such file or directory
    option(USE_INTERNAL_PARQUET_LIBRARY "Set to FALSE to use system parquet library instead of bundled" ${NOT_UNBUNDLED})
endif()

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/CMakeLists.txt")
    if(USE_INTERNAL_PARQUET_LIBRARY)
        message(WARNING "submodule contrib/arrow (required for Parquet) is missing. to fix try run: \n git submodule update --init --recursive")
    endif()
    set(USE_INTERNAL_PARQUET_LIBRARY 0)
    set(MISSING_INTERNAL_PARQUET_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_PARQUET_LIBRARY)
    find_package(Arrow)
    find_package(Parquet)
endif()

if(ARROW_INCLUDE_DIR AND PARQUET_INCLUDE_DIR)
elseif(NOT MISSING_INTERNAL_PARQUET_LIBRARY AND NOT OS_FREEBSD)
    include(cmake/find_snappy.cmake)
    set(CAN_USE_INTERNAL_PARQUET_LIBRARY 1)
    include(CheckCXXSourceCompiles)
    if(NOT USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY)
        set(CMAKE_REQUIRED_LIBRARIES ${DOUBLE_CONVERSION_LIBRARIES})
        set(CMAKE_REQUIRED_INCLUDES ${DOUBLE_CONVERSION_INCLUDE_DIR})
        check_cxx_source_compiles("
            #include <double-conversion/double-conversion.h>
            int main() { static const int flags_ = double_conversion::StringToDoubleConverter::ALLOW_CASE_INSENSIBILITY; return 0;}
        " HAVE_DOUBLE_CONVERSION_ALLOW_CASE_INSENSIBILITY)

        if(NOT HAVE_DOUBLE_CONVERSION_ALLOW_CASE_INSENSIBILITY) # HAVE_STD_RANDOM_SHUFFLE
            message(STATUS "Disabling internal parquet library because arrow is broken (can't use old double_conversion)")
            set(CAN_USE_INTERNAL_PARQUET_LIBRARY 0)
        endif()
    endif()

   if(NOT CAN_USE_INTERNAL_PARQUET_LIBRARY)
        set(USE_INTERNAL_PARQUET_LIBRARY 0)
   else()
    set(USE_INTERNAL_PARQUET_LIBRARY 1)

    if(USE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE)
        set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")
        set(PARQUET_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src" ${ClickHouse_BINARY_DIR}/contrib/arrow/cpp/src)
    endif()

    if(${USE_STATIC_LIBRARIES})
        set(ARROW_LIBRARY arrow_static)
        set(PARQUET_LIBRARY parquet_static)
        set(THRIFT_LIBRARY thrift_static)
    else()
        set(ARROW_LIBRARY arrow_shared)
        set(PARQUET_LIBRARY parquet_shared)
        if(USE_INTERNAL_PARQUET_LIBRARY_NATIVE_CMAKE)
            list(APPEND PARQUET_LIBRARY ${Boost_REGEX_LIBRARY})
        endif()
        set(THRIFT_LIBRARY thrift)
    endif()

    set(USE_PARQUET 1)
   endif()
endif()

endif()

if(USE_PARQUET)
    message(STATUS "Using Parquet: ${ARROW_LIBRARY}:${ARROW_INCLUDE_DIR} ; ${PARQUET_LIBRARY}:${PARQUET_INCLUDE_DIR} ; ${THRIFT_LIBRARY}")
else()
    message(STATUS "Building without Parquet support")
endif()
