if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/find_vectorclass.cmake)

set (CONFIG_VERSION ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_version.h)
set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config.h)
set (CONFIG_BUILD ${CMAKE_CURRENT_BINARY_DIR}/src/Common/config_build.cpp)

include (cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL}")
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config.h.in ${CONFIG_COMMON})
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config_version.h.in ${CONFIG_VERSION})

get_property (BUILD_COMPILE_DEFINITIONS DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY COMPILE_DEFINITIONS)
get_property (BUILD_INCLUDE_DIRECTORIES DIRECTORY ${ClickHouse_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
string (TIMESTAMP BUILD_DATE "%Y-%m-%d" UTC)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/src/Common/config_build.cpp.in ${CONFIG_BUILD})

if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

find_package (Threads)

add_subdirectory (src)

add_library(string_utils
    src/Common/StringUtils.h
    src/Common/StringUtils.cpp)
target_include_directories (string_utils PRIVATE ${DBMS_INCLUDE_DIR})

set(dbms_headers)
set(dbms_sources)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(dbms src/Parsers)
add_headers_and_sources(dbms src/Analyzers)
add_headers_and_sources(dbms src/Core)
add_headers_and_sources(dbms src/DataStreams)
add_headers_and_sources(dbms src/DataTypes)
add_headers_and_sources(dbms src/Databases)
add_headers_and_sources(dbms src/DataBases/Distributed)
add_headers_and_sources(dbms src/Dictionaries)
add_headers_and_sources(dbms src/Dictionaries/Embedded)
add_headers_and_sources(dbms src/Interpreters)
add_headers_and_sources(dbms src/Interpreters/ClusterProxy)
add_headers_and_sources(dbms src/Common)
add_headers_and_sources(dbms src/Common/HashTable)
add_headers_and_sources(dbms src/Common/ZooKeeper)
add_headers_and_sources(dbms src/IO)
add_headers_and_sources(dbms src/Columns)
add_headers_and_sources(dbms src/Storages)
add_headers_and_sources(dbms src/Storages/Distributed)
add_headers_and_sources(dbms src/Storages/MergeTree)
add_headers_and_sources(dbms src/Core)
add_headers_and_sources(dbms src/Client)
add_headers_only(dbms src/Server)

list (APPEND dbms_sources ${CONFIG_BUILD})
list (APPEND dbms_headers ${CONFIG_VERSION} ${CONFIG_COMMON})

list (APPEND dbms_sources src/Functions/IFunction.cpp src/Functions/FunctionFactory.cpp src/Functions/FunctionHelpers.cpp)
list (APPEND dbms_headers src/Functions/IFunction.h src/Functions/FunctionFactory.h src/Functions/FunctionHelpers.h)

list (APPEND dbms_sources
    src/AggregateFunctions/AggregateFunctionFactory.cpp
    src/AggregateFunctions/AggregateFunctionState.cpp
    src/AggregateFunctions/AggregateFunctionArray.cpp
    src/AggregateFunctions/AggregateFunctionNull.cpp
    src/AggregateFunctions/AggregateFunctionForEach.cpp
    src/AggregateFunctions/AggregateFunctionIf.cpp
    src/AggregateFunctions/AggregateFunctionMerge.cpp
    src/AggregateFunctions/AggregateFunctionCount.cpp
    src/AggregateFunctions/parseAggregateFunctionParameters.cpp)

list (APPEND dbms_headers
    src/AggregateFunctions/IAggregateFunction.h
    src/AggregateFunctions/AggregateFunctionFactory.h
    src/AggregateFunctions/AggregateFunctionState.h
    src/AggregateFunctions/AggregateFunctionArray.h
    src/AggregateFunctions/AggregateFunctionNull.h
    src/AggregateFunctions/AggregateFunctionForEach.h
    src/AggregateFunctions/AggregateFunctionIf.h
    src/AggregateFunctions/AggregateFunctionMerge.h
    src/AggregateFunctions/AggregateFunctionCount.h
    src/AggregateFunctions/parseAggregateFunctionParameters.h)

list (APPEND dbms_sources src/TableFunctions/TableFunctionFactory.cpp)
list (APPEND dbms_headers src/TableFunctions/ITableFunction.h src/TableFunctions/TableFunctionFactory.h)


list(REMOVE_ITEM dbms_sources
    src/Storages/StorageCloud.cpp
    src/Databases/DatabaseCloud.cpp
    src/Common/StringUtils.cpp)

if (APPLE OR CMAKE_SYSTEM MATCHES "FreeBSD")
    list(REMOVE_ITEM dbms_headers
        src/Common/AIO.h
        src/IO/WriteBufferAIO.h
        src/IO/ReadBufferAIO.h)
    list(REMOVE_ITEM dbms_sources
        src/IO/ReadBufferAIO.cpp
        src/IO/WriteBufferAIO.cpp)
endif()

if (MAKE_STATIC_LIBRARIES)
    add_library(dbms ${dbms_headers} ${dbms_sources})
else ()
    add_library(dbms SHARED ${dbms_headers} ${dbms_sources})
    set_target_properties (dbms PROPERTIES SOVERSION ${VERSION_MAJOR} VERSION ${VERSION_SO} OUTPUT_NAME clickhouse)
    install (TARGETS dbms LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT clickhouse)
endif ()


if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    set_source_files_properties(
        src/Functions/FunctionsArithmetic.cpp
        src/Functions/FunctionsArray.cpp
        src/Functions/FunctionsCoding.cpp
        src/Functions/FunctionsComparison.cpp
        src/Functions/FunctionsConditional.cpp
        src/Functions/FunctionsConversion.cpp
        src/Functions/FunctionsDateTime.cpp
        src/Functions/FunctionsDictionaries.cpp
        src/Functions/FunctionsFormatting.cpp
        src/Functions/FunctionsHashing.cpp
        src/Functions/FunctionsHigherOrder.cpp
        src/Functions/FunctionsLogical.cpp
        src/Functions/FunctionsRandom.cpp
        src/Functions/FunctionsReinterpret.cpp
        src/Functions/FunctionsRound.cpp
        src/Functions/FunctionsString.cpp
        src/Functions/FunctionsStringArray.cpp
        src/Functions/FunctionsStringSearch.cpp
        src/Functions/FunctionsURL.cpp
        src/Functions/FunctionsVisitParam.cpp
        src/Functions/FunctionsMath.cpp
        src/Functions/FunctionsGeo.cpp
        src/Functions/FunctionsMiscellaneous.cpp
        src/Functions/FunctionsTransform.cpp
        src/Dictionaries/FlatDictionary.cpp
        src/Dictionaries/HashedDictionary.cpp
        src/Dictionaries/CacheDictionary.cpp
        src/Dictionaries/TrieDictionary.cpp
        src/Dictionaries/RangeHashedDictionary.cpp
        src/Dictionaries/ComplexKeyHashedDictionary.cpp
        src/Dictionaries/ComplexKeyCacheDictionary.cpp
        PROPERTIES COMPILE_FLAGS -g0)
endif ()

if (NOT ARCH_ARM)
    set (LINK_LIBRARIES_ONLY_ON_X86_64 cpuid)
endif()

if (CMAKE_SYSTEM MATCHES "FreeBSD")
    set(PLATFORM_LIBS "execinfo")
else()
    set(PLATFORM_LIBS "")
endif()

target_link_libraries (dbms
    common
    ${MYSQLXX_LIBRARY}
    ${FARMHASH_LIBRARIES}
    ${METROHASH_LIBRARIES}
    ${LZ4_LIBRARY}
    ${ZSTD_LIBRARY}
    ${ZOOKEEPER_LIBRARY}
    string_utils
    ${DOUBLE_CONVERSION_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${LINK_LIBRARIES_ONLY_ON_X86_64}
    ${RE2_LIBRARY}
    ${RE2_ST_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    ${Poco_Data_LIBRARY}
    ${BTRIE_LIBRARIES}
)

if (Poco_DataODBC_FOUND)
    target_link_libraries (dbms ${Poco_DataODBC_LIBRARY})
endif()

if (Poco_MongoDB_FOUND)
    target_link_libraries (dbms ${Poco_MongoDB_LIBRARY})
endif()

if (Poco_NetSSL_FOUND)
    target_link_libraries (dbms ${Poco_NetSSL_LIBRARY})
endif()

target_link_libraries (dbms ${Poco_Foundation_LIBRARY})

if (USE_ICU)
    target_link_libraries (dbms ${ICU_LIBS})
endif ()

target_link_libraries (dbms
    ${PLATFORM_LIBS}
    ${CMAKE_DL_LIBS}
    ${CMAKE_THREAD_LIBS_INIT}
)

target_include_directories (dbms BEFORE PRIVATE ${DIVIDE_INCLUDE_DIR})
target_include_directories (dbms BEFORE PRIVATE ${SPARCEHASH_INCLUDE_DIR})
if (NOT USE_INTERNAL_LZ4_LIBRARY)
    target_include_directories (dbms BEFORE PRIVATE ${LZ4_INCLUDE_DIR})
endif ()
if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    target_include_directories (dbms BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()
target_include_directories (dbms PUBLIC ${DBMS_INCLUDE_DIR})
target_include_directories (dbms PUBLIC ${PCG_RANDOM_INCLUDE_DIR})

# only for copy_headers.sh:
target_include_directories (dbms PRIVATE ${COMMON_INCLUDE_DIR})
target_include_directories (dbms BEFORE PRIVATE ${DOUBLE_CONVERSION_INCLUDE_DIR})


if (ENABLE_TESTS)
    add_subdirectory (tests)
    # attach all dbms gtest sources
    grep_gtest_sources(${ClickHouse_SOURCE_DIR}/dbms dbms_gtest_sources)
    add_executable(unit_tests_dbms ${dbms_gtest_sources})
    target_link_libraries(unit_tests_dbms gtest_main dbms)
    add_check(unit_tests_dbms)
endif ()
