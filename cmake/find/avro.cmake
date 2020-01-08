option (ENABLE_AVRO "Enable Avro" ${ENABLE_LIBRARIES})

if (ENABLE_AVRO)

option (USE_INTERNAL_AVRO_LIBRARY "Set to FALSE to use system avro library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/avro/lang/c++/CMakeLists.txt")
    if(USE_INTERNAL_AVRO_LIBRARY)
        message(WARNING "submodule contrib/avro is missing. to fix try run: \n git submodule update --init --recursive")
    endif()
    set(MISSING_INTERNAL_AVRO_LIBRARY 1)
    set(USE_INTERNAL_AVRO_LIBRARY 0)
endif()

if (NOT USE_INTERNAL_AVRO_LIBRARY)
    find_package(Snappy REQUIRED)
    find_library(AVROCPP avrocpp)
elseif(NOT MISSING_INTERNAL_AVRO_LIBRARY)
    include(cmake/find/snappy.cmake)
    add_subdirectory(contrib/avro-cmake)
    set(AVROCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/avro/lang/c++/include")
    set(AVROCPP_LIBRARY avrocpp_s)
endif ()

if (AVROCPP_LIBRARY AND AVROCPP_INCLUDE_DIR)
    set(USE_AVRO 1)
endif()


# if (AVROCPP_LIBRARY AND AVROCPP_INCLUDE_DIR)
#     set(USE_AVROCPP 1)
# elseif (Boost_INCLUDE_DIRS AND SNAPPY_LIBRARY)
#     set(AVROCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/avro/lang/c++/include")
#     set(AVROCPP_LIBRARY avrocpp_s)
#     set(USE_AVROCPP 1)
# else()
#     set(USE_INTERNAL_AVROCPP_LIBRARY 0)
#     message(STATUS "avro deps: ${Boost_INCLUDE_DIRS}; ${SNAPPY_LIBRARY}; ${ZLIB_LIBRARY}")
# endif()

endif()

message (STATUS "Using avro=${USE_AVRO}: ${AVROCPP_LIBRARY} ${AVROCPP_INCLUDE_DIR}")
