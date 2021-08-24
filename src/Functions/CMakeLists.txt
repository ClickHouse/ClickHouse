configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

add_subdirectory(divide)

include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

target_link_libraries(clickhouse_functions
    PUBLIC
        ${BASE64_LIBRARY}
        ${CITYHASH_LIBRARIES}
        ${FARMHASH_LIBRARIES}
        ${FASTOPS_LIBRARY}
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        consistent-hashing
        dbms
        metrohash
        murmurhash

    PRIVATE
        ${ZLIB_LIBRARIES}
        boost::filesystem
        divide_impl
)

if (OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries(clickhouse_functions PUBLIC ${OPENSSL_CRYPTO_LIBRARY})
endif()

target_include_directories(clickhouse_functions SYSTEM PRIVATE ${SPARSEHASH_INCLUDE_DIR})

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO"
    OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    set (STRIP_DSF_DEFAULT ON)
else()
    set (STRIP_DSF_DEFAULT OFF)
endif()


#  Provides faster linking and lower binary size.
#  Tradeoff is the inability to debug some source files with e.g. gdb
#  (empty stack frames and no local variables)."
option(STRIP_DEBUG_SYMBOLS_FUNCTIONS "Do not generate debugger info for ClickHouse functions" ${STRIP_DSF_DEFAULT})

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    message(WARNING "Not generating debugger info for ClickHouse functions")
    target_compile_options(clickhouse_functions PRIVATE "-g0")
else()
    message(STATUS "Generating debugger info for ClickHouse functions")
endif()

if (USE_ICU)
    target_link_libraries (clickhouse_functions PRIVATE ${ICU_LIBRARIES})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${ICU_INCLUDE_DIRS})
endif ()

if (USE_FASTOPS)
    target_include_directories (clickhouse_functions SYSTEM PRIVATE ${FASTOPS_INCLUDE_DIR})
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
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

# ClickHouse developers may use platform-dependent code under some macro (e.g. `#ifdef ENABLE_MULTITARGET`).
# If turned ON, this option defines such macro.
# See `src/Functions/TargetSpecific.h`
option(ENABLE_MULTITARGET_CODE "Enable platform-dependent code" ON)

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

add_subdirectory(JSONPath)

if (USE_STATS)
    target_link_libraries(clickhouse_functions PRIVATE stats)
endif()

# Signed integer overflow on user-provided data inside boost::geometry - ignore.
set_source_files_properties("pointInPolygon.cpp" PROPERTIES COMPILE_FLAGS -fno-sanitize=signed-integer-overflow)

# target_link_libraries(clickhouse_functions PRIVATE ${S2_LIBRARY})
target_include_directories(clickhouse_functions SYSTEM PUBLIC ${S2_GEOMETRY_INCLUDE_DIR})
