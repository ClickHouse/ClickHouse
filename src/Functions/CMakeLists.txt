configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunctionImpl.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunctionImpl.h FunctionFactory.h FunctionHelpers.h)

if (NOT USE_H3)
    list (REMOVE_ITEM clickhouse_functions_sources
        geoToH3.cpp
        h3EdgeAngle.cpp
        h3EdgeLengthM.cpp
        h3GetBaseCell.cpp
        h3GetResolution.cpp
        h3HexAreaM2.cpp
        h3IndexesAreNeighbors.cpp
        h3IsValid.cpp
        h3kRing.cpp
        h3ToChildren.cpp
        h3ToParent.cpp
        h3ToString.cpp
        stringToH3.cpp
    )
endif ()

add_library(clickhouse_functions ${clickhouse_functions_sources})

target_link_libraries(clickhouse_functions
    PUBLIC
        ${BASE64_LIBRARY}
        ${CITYHASH_LIBRARIES}
        ${FARMHASH_LIBRARIES}
        ${FASTOPS_LIBRARY}
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        consistent-hashing
        consistent-hashing-sumbur
        dbms
        metrohash
        murmurhash

    PRIVATE
        ${ZLIB_LIBRARIES}
        boost::filesystem
        libdivide
)

if (OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries(clickhouse_functions PUBLIC ${OPENSSL_CRYPTO_LIBRARY})
endif()

target_include_directories(clickhouse_functions SYSTEM PRIVATE ${SPARSEHASH_INCLUDE_DIR})

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif ()

if (USE_ICU)
    target_link_libraries (clickhouse_functions PRIVATE ${ICU_LIBRARIES})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${ICU_INCLUDE_DIRS})
endif ()

if (USE_FASTOPS)
    target_include_directories (clickhouse_functions SYSTEM PRIVATE ${FASTOPS_INCLUDE_DIR})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

if (USE_EMBEDDED_COMPILER)
    target_link_libraries(clickhouse_functions PRIVATE ${REQUIRED_LLVM_LIBRARIES})
    target_include_directories(clickhouse_functions SYSTEM BEFORE PUBLIC ${LLVM_INCLUDE_DIRS})
endif ()

if(USE_BASE64)
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${BASE64_INCLUDE_DIR})
endif()

target_link_libraries(clickhouse_functions PRIVATE lz4)

if (USE_H3)
    target_link_libraries(clickhouse_functions PRIVATE ${H3_LIBRARY})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${H3_INCLUDE_DIR})
endif()

target_link_libraries(clickhouse_functions PRIVATE hyperscan)

if(USE_SIMDJSON)
    target_link_libraries(clickhouse_functions PRIVATE simdjson)
endif()

if(USE_RAPIDJSON)
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${RAPIDJSON_INCLUDE_DIR})
endif()

option(ENABLE_MULTITARGET_CODE "" ON)
if (ENABLE_MULTITARGET_CODE)
    add_definitions(-DENABLE_MULTITARGET_CODE=1)
else()
    add_definitions(-DENABLE_MULTITARGET_CODE=0)
endif()

add_subdirectory(GatherUtils)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_gatherutils)

add_subdirectory(URL)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_url)

add_subdirectory(array)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_array)

if (USE_STATS)
    target_link_libraries(clickhouse_functions PRIVATE stats)
endif()
