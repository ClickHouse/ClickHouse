configure_file(config_functions.h.in ${ConfigIncludePath}/config_functions.h)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions .)

list(REMOVE_ITEM clickhouse_functions_sources IFunctionImpl.cpp FunctionFactory.cpp FunctionHelpers.cpp)
list(REMOVE_ITEM clickhouse_functions_headers IFunctionImpl.h FunctionFactory.h FunctionHelpers.h)

add_library(clickhouse_functions ${clickhouse_functions_sources})

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
target_precompile_headers(clickhouse_functions
                          PRIVATE
                          <algorithm>
                          <any>
                          <array>
                          <atomic>
                          <bitset>
                          <cassert>
                          <cctype>
                          <cerrno>
                          <chrono>
                          <climits>
                          <cmath>
                          <csignal>
                          <cstdarg>
                          <cstddef>
                          <cstdint>
                          <cstdio>
                          <cstdlib>
                          <cstring>
                          <ctime>
                          <deque>
                          <exception>
                          <filesystem>
                          <fstream>
                          <functional>
                          <future>
                          <iomanip>
                          <iosfwd>
                          <iostream>
                          <iterator>
                          <limits>
                          <list>
                          <map>
                          <memory>
                          <mutex>
                          <new>
                          <numeric>
                          <optional>
                          <ostream>
                          <queue>
                          <random>
                          <regex>
                          <set>
                          <sstream>
                          <stack>
                          <stdexcept>
                          <streambuf>
                          <string>
                          <thread>
                          <tuple>
                          <typeindex>
                          <typeinfo>
                          <unordered_set>
                          <utility>
                          <variant>
                          <vector>

                          <boost/algorithm/hex.hpp>
                          <boost/algorithm/string.hpp>
                          <boost/algorithm/string/case_conv.hpp>
                          <boost/algorithm/string/find_iterator.hpp>
                          <boost/algorithm/string/finder.hpp>
                          <boost/algorithm/string/join.hpp>
                          <boost/algorithm/string/predicate.hpp>
                          <boost/algorithm/string/replace.hpp>
                          <boost/algorithm/string/split.hpp>
                          <boost/algorithm/string/trim.hpp>
                          <boost/assert.hpp>
                          <boost/blank.hpp>
                          <boost/container/flat_map.hpp>
                          <boost/core/noncopyable.hpp>
                          <boost/format.hpp>
                          <boost/functional/hash.hpp>
                          <boost/geometry.hpp>
                          <boost/geometry/algorithms/comparable_distance.hpp>
                          <boost/geometry/geometries/multi_polygon.hpp>
                          <boost/geometry/geometries/point_xy.hpp>
                          <boost/geometry/geometries/polygon.hpp>
                          <boost/geometry/geometries/segment.hpp>
                          <boost/geometry/strategies/cartesian/distance_pythagoras.hpp>
                          <boost/intrusive/list.hpp>
                          <boost/intrusive/set.hpp>
                          <boost/iterator/counting_iterator.hpp>
                          <boost/iterator/transform_iterator.hpp>
                          <boost/iterator_adaptors.hpp>
                          <boost/lockfree/queue.hpp>
                          <boost/multi_index/global_fun.hpp>
                          <boost/multi_index/member.hpp>
                          <boost/multi_index/ordered_index.hpp>
                          <boost/multi_index/sequenced_index.hpp>
                          <boost/multi_index_container.hpp>
                          <boost/noncopyable.hpp>
                          <boost/numeric/conversion/cast.hpp>
                          <boost/operators.hpp>
                          <boost/program_options.hpp>
                          <boost/program_options/options_description.hpp>
                          <boost/range/adaptor/filtered.hpp>
                          <boost/range/adaptor/indexed.hpp>
                          <boost/range/adaptor/map.hpp>
                          <boost/range/adaptor/reversed.hpp>
                          <boost/range/algorithm/copy.hpp>
                          <boost/range/algorithm/fill.hpp>
                          <boost/range/algorithm/find.hpp>
                          <boost/range/algorithm/find_first_of.hpp>
                          <boost/range/algorithm/for_each.hpp>
                          <boost/range/algorithm/lower_bound.hpp>
                          <boost/range/algorithm/stable_sort.hpp>
                          <boost/range/algorithm_ext/erase.hpp>
                          <boost/range/irange.hpp>
                          <boost/range/iterator_range.hpp>
                          <boost/range/iterator_range_core.hpp>
                          <boost/range/join.hpp>
                          <boost/rational.hpp>
                          <boost/smart_ptr/make_shared.hpp>
                          <boost/smart_ptr/intrusive_ptr.hpp>
                          <boost/smart_ptr/intrusive_ref_counter.hpp>
                          <boost/uuid/nil_generator.hpp>
                          <boost/uuid/random_generator.hpp>
                          <boost/uuid/uuid.hpp>

                          <Poco/Foundation.h>
                          <Poco/Bugcheck.h>

                          <Columns/IColumn.h>
                          <Common/PODArray.h>
                          <Core/Field.h>
                          <Core/Block.h>
                          <Functions/IFunction.h>
                          <Functions/FunctionFactory.h>
                          <Storages/IStorage.h>
                          <DataTypes/IDataType.h>
                          )
