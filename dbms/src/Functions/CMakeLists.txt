configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunction.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunction.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

execute_process(COMMAND /usr/bin/ccache /usr/bin/g++-9  -DBOOST_SYSTEM_NO_DEPRECATED -DPOCO_STATIC -DPOCO_UNBUNDLED_ZLIB -DUNALIGNED_OK -DWITH_GZFILEOP -DX86_64 -DZLIB_COMPAT -I${CMAKE_BINARY_DIR}/dbms/includes/configs -I${CMAKE_BINARY_DIR}/dbms/src/Core/include -I${CMAKE_SOURCE_DIR}/dbms/src -I${CMAKE_BINARY_DIR}/dbms/src -I${CMAKE_SOURCE_DIR}/libs/libcommon/include -I${CMAKE_BINARY_DIR}/libs/libcommon/include -I${CMAKE_SOURCE_DIR}/contrib/zlib-ng -I${CMAKE_BINARY_DIR}/contrib/zlib-ng -I${CMAKE_SOURCE_DIR}/contrib/cityhash102/include -I${CMAKE_SOURCE_DIR}/contrib/croaring -I${CMAKE_SOURCE_DIR}/libs/libmysqlxx/include -I${CMAKE_SOURCE_DIR}/contrib/mariadb-connector-c/include -I${CMAKE_BINARY_DIR}/contrib/mariadb-connector-c/include -I${CMAKE_SOURCE_DIR}/libs/consistent-hashing -I${CMAKE_SOURCE_DIR}/libs/consistent-hashing-sumbur -I${CMAKE_SOURCE_DIR}/contrib/libfarmhash -I${CMAKE_SOURCE_DIR}/contrib/libmetrohash/src -I${CMAKE_SOURCE_DIR}/contrib/murmurhash/include -isystem ${CMAKE_SOURCE_DIR}/contrib/libunwind/include -isystem ${CMAKE_SOURCE_DIR}/contrib/pdqsort -isystem ${CMAKE_SOURCE_DIR}/contrib/libpcg-random/include -isystem ${CMAKE_SOURCE_DIR}/contrib/double-conversion -isystem ${CMAKE_SOURCE_DIR}/contrib/libhdfs3/include -isystem ${CMAKE_SOURCE_DIR}/contrib/re2 -isystem ${CMAKE_SOURCE_DIR}/contrib/poco/Util/include -isystem ${CMAKE_SOURCE_DIR}/contrib/poco/Foundation/include -isystem ${CMAKE_SOURCE_DIR}/contrib/poco/XML/include -isystem ${CMAKE_SOURCE_DIR}/contrib/poco/JSON/include -isystem ${CMAKE_SOURCE_DIR}/contrib/boost -isystem ${CMAKE_SOURCE_DIR}/contrib/poco/Net/include -isystem ${CMAKE_BINARY_DIR}/contrib/re2_st -isystem ${CMAKE_SOURCE_DIR}/contrib/fastops  -fsized-deallocation  -pipe -msse4.1 -msse4.2 -mpopcnt  -fno-omit-frame-pointer  -Wall  -Wnon-virtual-dtor -Wno-array-bounds -Wextra -Werror -O2 -g -DNDEBUG -O3    -D OS_LINUX -fno-tree-loop-distribute-patterns -Wbool-compare -Wcast-align -Wcast-qual -Wdelete-incomplete -Wdisabled-optimization -Wduplicated-cond -Wenum-compare -Winit-self -Wlogical-not-parentheses -Wlogical-op -Wmaybe-uninitialized -Wmisleading-indentation -Wmissing-declarations -Wnon-virtual-dtor -Wno-return-local-addr -Wnull-dereference -Wodr -Wold-style-cast -Wplacement-new=2 -Wpointer-arith -Wredundant-decls -Wreorder -Wshadow -Wshift-negative-value -Wsized-deallocation -Wsizeof-array-argument -Wsizeof-pointer-memaccess -Wsuggest-override -Wswitch-bool -Wtautological-compare -Wtrampolines -Wunused -Wvector-operation-performance -E -std=c++17 -c ${CMAKE_SOURCE_DIR}/dbms/src/Functions/abs.cpp)
#add_library(clickhouse_functions_foo abs.cpp)
#set_target_properties(clickhouse_functions_foo PROPERTIES COMPILE_FLAGS "-E")
#target_compile_options(clickhouse_functions_foo PRIVATE "-E")
#target_include_directories(clickhouse_functions_foo SYSTEM PRIVATE ../ ../Core/include/ . ${ConfigIncludePath} ${DIVIDE_INCLUDE_DIR} ${METROHASH_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR})
#target_link_libraries(clickhouse_functions_foo
#                      PUBLIC
#                      clickhouse_dictionaries
#                      clickhouse_dictionaries_embedded
#                      dbms
#                      ${CONSISTENT_HASHING_LIBRARY}
#                      consistent-hashing-sumbur
#                      ${CITYHASH_LIBRARIES}
#                      ${FARMHASH_LIBRARIES}
#                      ${METROHASH_LIBRARIES}
#                      murmurhash
#                      ${BASE64_LIBRARY}
#                      ${FASTOPS_LIBRARY}
#
#                      PRIVATE
#                      ${ZLIB_LIBRARIES}
#                      ${Boost_FILESYSTEM_LIBRARY}
#                      ${CMAKE_DL_LIBS}
#                      )

target_link_libraries(clickhouse_functions
    PUBLIC
        clickhouse_dictionaries
        clickhouse_dictionaries_embedded
        dbms
        ${CONSISTENT_HASHING_LIBRARY}
        consistent-hashing-sumbur
        ${CITYHASH_LIBRARIES}
        ${FARMHASH_LIBRARIES}
        ${METROHASH_LIBRARIES}
        murmurhash
        ${BASE64_LIBRARY}
        ${FASTOPS_LIBRARY}

    PRIVATE
        ${ZLIB_LIBRARIES}
        ${Boost_FILESYSTEM_LIBRARY}
        ${CMAKE_DL_LIBS}
)

if (OPENSSL_CRYPTO_LIBRARY)
    target_link_libraries(clickhouse_functions PUBLIC ${OPENSSL_CRYPTO_LIBRARY})
endif()

target_include_directories(clickhouse_functions SYSTEM PRIVATE ${DIVIDE_INCLUDE_DIR} ${METROHASH_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR})

if (CONSISTENT_HASHING_INCLUDE_DIR)
    target_include_directories (clickhouse_functions PRIVATE ${CONSISTENT_HASHING_INCLUDE_DIR})
endif ()

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions PRIVATE "-g0")
endif ()

if (USE_ICU)
    target_link_libraries (clickhouse_functions PRIVATE ${ICU_LIBRARIES})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${ICU_INCLUDE_DIRS})
endif ()

if (USE_VECTORCLASS)
    target_include_directories (clickhouse_functions SYSTEM PRIVATE ${VECTORCLASS_INCLUDE_DIR})
endif ()

if (USE_FASTOPS)
    target_include_directories (clickhouse_functions SYSTEM PRIVATE ${FASTOPS_INCLUDE_DIR})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

if (USE_EMBEDDED_COMPILER)
    llvm_libs_all(REQUIRED_LLVM_LIBRARIES)
    target_link_libraries(clickhouse_functions PRIVATE ${REQUIRED_LLVM_LIBRARIES})
    target_include_directories(clickhouse_functions SYSTEM BEFORE PUBLIC ${LLVM_INCLUDE_DIRS})
endif ()

if(USE_BASE64)
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${BASE64_INCLUDE_DIR})
endif()

if(USE_XXHASH)
    target_link_libraries(clickhouse_functions PRIVATE ${XXHASH_LIBRARY})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${XXHASH_INCLUDE_DIR})
endif()

if (USE_H3)
    target_link_libraries(clickhouse_functions PRIVATE ${H3_LIBRARY})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${H3_INCLUDE_DIR})
endif()

if(USE_HYPERSCAN)
    target_link_libraries(clickhouse_functions PRIVATE ${HYPERSCAN_LIBRARY})
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${HYPERSCAN_INCLUDE_DIR})
endif()

if(USE_SIMDJSON)
    target_link_libraries(clickhouse_functions PRIVATE ${SIMDJSON_LIBRARY})
endif()

if(USE_RAPIDJSON)
    target_include_directories(clickhouse_functions SYSTEM PRIVATE ${RAPIDJSON_INCLUDE_DIR})
endif()

add_subdirectory(GatherUtils)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_gatherutils)

add_subdirectory(URL)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_url)

add_subdirectory(array)
target_link_libraries(clickhouse_functions PRIVATE clickhouse_functions_array)
