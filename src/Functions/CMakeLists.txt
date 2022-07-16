include(configure_config.cmake)
configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

add_subdirectory(divide)

include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

target_link_libraries(clickhouse_functions
    PUBLIC
        ch_contrib::wyhash
        ch_contrib::cityhash
        ch_contrib::farmhash
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        ch_contrib::consistent_hashing
        dbms
        ch_contrib::metrohash
        ch_contrib::murmurhash
        ch_contrib::hashidsxx

    PRIVATE
        ch_contrib::zlib
        boost::filesystem
        divide_impl
)

if (TARGET OpenSSL::Crypto)
    target_link_libraries(clickhouse_functions PUBLIC OpenSSL::Crypto)
endif()

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
    message(INFO "Not generating debugger info for ClickHouse functions")
    target_compile_options(clickhouse_functions PRIVATE "-g0")
else()
    message(STATUS "Generating debugger info for ClickHouse functions")
endif()

if (TARGET ch_contrib::icu)
    target_link_libraries (clickhouse_functions PRIVATE ch_contrib::icu)
endif ()

if (TARGET ch_contrib::fastops)
    target_link_libraries (clickhouse_functions PRIVATE ch_contrib::fastops)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()

if (TARGET ch_contrib::llvm)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::llvm)
endif ()

if (TARGET ch_contrib::base64)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::base64)
endif()

target_link_libraries(clickhouse_functions PRIVATE ch_contrib::lz4)

if (ENABLE_NLP)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::cld2)
endif()

if (TARGET ch_contrib::h3)
    target_link_libraries (clickhouse_functions PRIVATE ch_contrib::h3)
endif()

if (TARGET ch_contrib::vectorscan)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::vectorscan)
endif()

if (TARGET ch_contrib::simdjson)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    target_link_libraries(clickhouse_functions PRIVATE ch_contrib::rapidjson)
endif()

add_subdirectory(GatherUtils)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_gatherutils)

add_subdirectory(URL)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_url)

add_subdirectory(array)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_array)

add_subdirectory(JSONPath)

# Signed integer overflow on user-provided data inside boost::geometry - ignore.
set_source_files_properties("pointInPolygon.cpp" PROPERTIES COMPILE_FLAGS -fno-sanitize=signed-integer-overflow)

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
endif ()
