include(configure_config.cmake)
configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

add_subdirectory(divide)

include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp extractTimeZoneFromFunctionArguments.cpp replicate.cpp FunctionsLogical.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h extractTimeZoneFromFunctionArguments.h replicate.h FunctionsLogical.h)

add_library(clickhouse_functions_obj OBJECT ${clickhouse_functions_sources})
add_library(clickhouse_functions INTERFACE)

list (APPEND PUBLIC_LIBS
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
)

list (APPEND PRIVATE_LIBS
        ch_contrib::zlib
        boost::filesystem
        divide_impl
)

if (TARGET OpenSSL::Crypto)
    list (APPEND PUBLIC_LIBS OpenSSL::Crypto)
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
    target_compile_options(clickhouse_functions_obj PRIVATE "-g0")
else()
    message(STATUS "Generating debugger info for ClickHouse functions")
endif()

if (TARGET ch_contrib::icu)
    list (APPEND PRIVATE_LIBS ch_contrib::icu)
endif ()

if (TARGET ch_contrib::fastops)
    list (APPEND PRIVATE_LIBS ch_contrib::fastops)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif ()

if (TARGET ch_contrib::llvm)
    list (APPEND PRIVATE_LIBS ch_contrib::llvm)
endif ()

if (TARGET ch_contrib::base-x)
    list (APPEND PRIVATE_LIBS ch_contrib::base-x)
endif()

if (TARGET ch_contrib::base64)
    list (APPEND PRIVATE_LIBS ch_contrib::base64)
endif()

list (APPEND PRIVATE_LIBS ch_contrib::lz4)

if (ENABLE_NLP)
    list (APPEND PRIVATE_LIBS ch_contrib::cld2)
endif()

if (TARGET ch_contrib::h3)
    list (APPEND PRIVATE_LIBS ch_contrib::h3)
endif()

if (TARGET ch_contrib::vectorscan)
    list (APPEND PRIVATE_LIBS ch_contrib::vectorscan)
endif()

if (TARGET ch_contrib::simdjson)
    list (APPEND PRIVATE_LIBS ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    list (APPEND PRIVATE_LIBS ch_contrib::rapidjson)
endif()

add_subdirectory(GatherUtils)
list (APPEND PRIVATE_LIBS clickhouse_functions_gatherutils)

add_subdirectory(URL)
list (APPEND PRIVATE_LIBS clickhouse_functions_url)

add_subdirectory(array)
list (APPEND PRIVATE_LIBS clickhouse_functions_array)

add_subdirectory(JSONPath)
list (APPEND PRIVATE_LIBS clickhouse_functions_jsonpath)

# Signed integer overflow on user-provided data inside boost::geometry - ignore.
set_source_files_properties("pointInPolygon.cpp" PROPERTIES COMPILE_FLAGS -fno-sanitize=signed-integer-overflow)

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
endif ()

target_link_libraries(clickhouse_functions_obj PUBLIC ${PUBLIC_LIBS} PRIVATE ${PRIVATE_LIBS})
target_link_libraries(clickhouse_functions INTERFACE $<TARGET_OBJECTS:clickhouse_functions_obj> ${PUBLIC_LIBS} ${PRIVATE_LIBS})
