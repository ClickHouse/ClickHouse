include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
include(${ClickHouse_SOURCE_DIR}/cmake/generate_code.cmake)

add_headers_and_sources(clickhouse_dictionaries .)

generate_code(ComplexKeyCacheDictionary_generate1 UInt8 UInt16 UInt32 UInt64 UInt128 Int8 Int16 Int32 Int64 Float32 Float64 Decimal32 Decimal64 Decimal128)
generate_code(ComplexKeyCacheDictionary_generate2 UInt8 UInt16 UInt32 UInt64 UInt128 Int8 Int16 Int32 Int64 Float32 Float64 Decimal32 Decimal64 Decimal128)
generate_code(ComplexKeyCacheDictionary_generate3 UInt8 UInt16 UInt32 UInt64 UInt128 Int8 Int16 Int32 Int64 Float32 Float64 Decimal32 Decimal64 Decimal128)
generate_code(CacheDictionary_generate1 UInt8 UInt16 UInt32 UInt64 UInt128 Int8 Int16 Int32 Int64 Float32 Float64 Decimal32 Decimal64 Decimal128)
generate_code(CacheDictionary_generate2 UInt8 UInt16 UInt32 UInt64 UInt128 Int8 Int16 Int32 Int64 Float32 Float64 Decimal32 Decimal64 Decimal128)
generate_code(CacheDictionary_generate3 UInt8 UInt16 UInt32 UInt64 UInt128 Int8 Int16 Int32 Int64 Float32 Float64 Decimal32 Decimal64 Decimal128)
add_headers_and_sources(clickhouse_dictionaries ${CMAKE_CURRENT_BINARY_DIR}/generated/)

list(REMOVE_ITEM clickhouse_dictionaries_sources DictionaryFactory.cpp DictionarySourceFactory.cpp DictionaryStructure.cpp getDictionaryConfigurationFromAST.cpp)
list(REMOVE_ITEM clickhouse_dictionaries_headers DictionaryFactory.h DictionarySourceFactory.h DictionaryStructure.h getDictionaryConfigurationFromAST.h)

add_library(clickhouse_dictionaries ${clickhouse_dictionaries_sources})
target_link_libraries(clickhouse_dictionaries PRIVATE dbms clickhouse_common_io string_utils ${BTRIE_LIBRARIES})

if(Poco_SQL_FOUND AND NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
endif()

if(USE_POCO_SQLODBC)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_SQLODBC_LIBRARY} ${Poco_SQL_LIBRARY})
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_SQLODBC_INCLUDE_DIR} ${Poco_SQL_INCLUDE_DIR})
endif()

if(Poco_Data_FOUND)
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
endif()

if(USE_POCO_DATAODBC)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_DataODBC_LIBRARY} ${Poco_Data_LIBRARY})
    target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_DataODBC_INCLUDE_DIR})
endif()

if(USE_POCO_MONGODB)
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_MongoDB_LIBRARY})
endif()

if(USE_POCO_REDIS)
    # for code highlighting in CLion
    # target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${Poco_Redis_INCLUDE_DIR})

    # for build
    target_link_libraries(clickhouse_dictionaries PRIVATE ${Poco_Redis_LIBRARY})
endif()

add_subdirectory(Embedded)

target_include_directories(clickhouse_dictionaries SYSTEM PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_precompile_headers(clickhouse_dictionaries
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
                          <boost/range/algorithm/find_if.hpp>
                          <boost/range/algorithm/find_first_of.hpp>
                          <boost/range/algorithm/for_each.hpp>
                          <boost/range/algorithm/lower_bound.hpp>
                          <boost/range/algorithm/sort.hpp>
                          <boost/range/algorithm/stable_sort.hpp>
                          <boost/range/algorithm/upper_bound.hpp>
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
